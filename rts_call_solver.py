from pyomo.environ import *
from pyomo.opt import SolverFactory
import pandas as pd

from conf import Config
import gurobipy

from pyomo.util.infeasible import log_infeasible_constraints
import logging

class ModelSolver(object):
    def __init__(self, model, loop):
        self.model = model
        self.loop = loop
        self.solver_type = Config.OPTIMISATION_MODELLING_CONFIG['solver_type']
        self.__solve()

    def __solve(self):
        print("[ModelSolver] Solver object initiated...")
        TransformationFactory("contrib.detect_fixed_vars").apply_to(self.model)
        TransformationFactory("contrib.deactivate_trivial_constraints").apply_to(self.model)

        opt = SolverFactory(self.solver_type, solver_io="lp")  # finding solver
        for k, v in Config.OPTIMISATION_MODELLING_CONFIG['solver_option'].get(
                self.solver_type).items():  # setting solver parameters, if any found in config
            opt.options[k] = v
        try:
            print("[ModelSolver] Solver starting...")
            print("import gurobipy", gurobipy)
            # solver_parameters = "ResultFile=model.ilp" # write an ILP file to print the IIS
            # self.model.write(f'rts_{self.loop}.lp', io_options={'symbolic_solver_labels': True})
            results = opt.solve(self.model, tee=True, symbolic_solver_labels=True)
            # results = opt.solve(self.model, tee=True, options_string=solver_parameters, symbolic_solver_labels=True)
            # log_infeasible_constraints(self.model, log_expression=True, log_variables=True)
            # logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.INFO)
            print("[ModelSolver] Solver completed.")
        except  Exception as e:
            print("Could not execute solve remotely", "error = ", e)
            raise e
            # opt = SolverFactory(self.solver_type, 
            #                        executable=Config.OPTIMISATION_MODELLING_CONFIG['solver_loc'].get(self.solver_type)) # finding solver with path given, for local run
            # for k, v in Config.OPTIMISATION_MODELLING_CONFIG['solver_option'].get(self.solver_type).items():  # setting solver parameters, if any found in config
            #     opt.options[k] = v
            # try:
            #     results = opt.solve(self.model, tee=True)
            #     print("[ModelSolver] Solver completed.")
            # except Exception as e:
            #     raise Exception(f"Model optimisation failed with {self.solver_type} with error message {e}.")

        if (results.solver.status == SolverStatus.ok) and (
                results.solver.termination_condition == TerminationCondition.optimal):
            print("Solution is feasible and optimal")
            results.write()
        elif results.solver.termination_condition == TerminationCondition.infeasible:
            raise ValueError("Model optimisation resulted into an infeasible solution")
        self.model.optimised = True

        # for v in self.model.component_objects(Var, active=True):
        #     print ("Variable component object",v)
        #     print ("Type of component object: ", str(type(v))[1:-1]) # Stripping <> for nbconvert
        #     varobject = getattr(self.model, str(v))
        #     print ("Type of object accessed via getattr: ", str(type(varobject))[1:-1])
        #     for index in varobject:
        #         print ("   ", index, varobject[index].value)
        return self.model

    def get_output(self, model):
        data = []
        for r in model.R:
            for k in model.K:
                for v in model.V:
                    if model.y[r, k, v].value == 1:
                        data.append({"Time start route": model.d[k, v].value,
                                     "Time complete route": model.d[k, v].value + model.tr[r],
                                     "Route_ID": r,
                                     "Trip": v,
                                     "Vehicle Number": k,
                                     "Tanker_Capacity": model.Q[k],
                                     "visit_station_or_not": model.y[r, k, v].value})
        result_df = pd.DataFrame(data)
        return result_df
