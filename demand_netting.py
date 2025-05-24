import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import floor 

# --- Assumed helper functions and classes (based on task description) ---
def string_to_int(value, default=None):
    if value is None: return default
    try:
        return int(value)
    except ValueError:
        raise 
    except TypeError: 
        raise

def string_to_bool(value, default=None):
    if value is None: return default
    if isinstance(value, bool): return value
    s_val = str(value).lower()
    if s_val in ('true', '1', 'yes', 'y'): return True
    if s_val in ('false', '0', 'no', 'n'): return False
    raise ValueError(f"Cannot convert '{value}' to boolean.")

def to_key_col(value: str):
    if not isinstance(value, str): 
        raise TypeError("Input to to_key_col must be a string.")
    if len(value) < 3: 
        raise IndexError("String too short for to_key_col operation.")
    return value[:3].upper() 

class PluginException(Exception):
    """Custom exception for plugin errors."""
    def __init__(self, message):
        super().__init__(message)

class Config:
    """Placeholder for configuration parameter keys."""
    ORDER_HORIZON = "OrderHorizon"
    USE_ORDER_FORECAST_MAP = "UseOrderForecastMap"
    TIME_HIERARCHY_DATA = "TimeHierarchyData" 
    TIME_COLUMN_KEY = "TimeColumnKey" 
    HIERARCHY_COL = "HierarchyColumn" 
    DEMAND_TYPE = "DemandType"
    ORDER_COVER_BY_RTF = "OrderCoverByRTF"
    REMAINING_FORECAST_HEADER = "SumRemainingForecast"
    FORECAST_REMAINING = "ForecastRemaining" 
    REMAINING_ORDER_AFTER_FORECAST = "RemainingOrderAfterForecast"
    ORDER_QTY_COL = "OrderQuantity" 
    FORECAST_STREAM_PARAMS = "ForecastStreamParameters" 
    BASIS_COL = "BasisColumn" 
    TELESCOPIC_COL = "TelescopicColumn" 
    PROFILE_METHOD_COL = "ProfileMethod" 
    ORDER_DUE_DATE_COL = "OrderDueDate" 
    FINAL_TIME_ATTR_COL = "FinalTimeAttribute" 
    NETTED_DEMAND_QTY_COL = "NettedDemandQuantity"
    DEMAND_ID_COL = "DemandID"


# --- DemandNetting Class ---
class DemandNetting:
    def __init__(self, parameters: dict):
        self.class_name = self.__class__.__name__
        self.parameters = parameters 
        self.TIME = "Time" 
        self.UPWARD_ITEM = "UpwardItem" 
        self.FORECAST_QTY_COL = "ForecastQuantity" 
        self.HIERARCHY_COL = self.parameters.get(Config.HIERARCHY_COL, "DefaultHierarchyCol") 
        self.DEMAND_TYPE = self.parameters.get(Config.DEMAND_TYPE, "DefaultDemandTypeCol")
        self.DEMAND_ID = self.parameters.get(Config.DEMAND_ID_COL, "DemandId") 
        self.orderCoverByRTF = self.parameters.get(Config.ORDER_COVER_BY_RTF, "DefaultOrderCoverCol")
        self.sumRemForecastHeader = self.parameters.get(Config.REMAINING_FORECAST_HEADER, "SumRemFcst")
        self.forecast_remaining = self.parameters.get(Config.FORECAST_REMAINING, "FcstRem")
        self.remaining_order_after_forecast = self.parameters.get(Config.REMAINING_ORDER_AFTER_FORECAST, "RemOrder")
        self.order_qty = self.parameters.get(Config.ORDER_QTY_COL, "DefaultOrderQtyCol")

        self.order_horizon = None
        self.use_order_forecast_map = None
        self.time_hierarchy_data = None 
        self.in_orders = pd.DataFrame() 
        self.forecast = pd.DataFrame() 
        self.time_priority_data = {}
        self.all_time_buckets = [] 
        self._map = {} 
        self.orderQtyHash = {} 
        self.forecastQtyHash = {} 
        self.enable_time_hierarchy = self.parameters.get("EnableTimeHierarchy", False) 

        self.plugin_log("Initializing DemandNetting", _type="info")
        try:
            val = self.parameters.get(Config.ORDER_HORIZON)
            self.order_horizon = string_to_int(val)
        except ValueError as e: 
            self.plugin_log(f"Error initializing order_horizon: Cannot convert value '{val}' to int. Details: {e}", _type="error")
            raise PluginException(f"Error initializing order_horizon: Cannot convert value '{val}' to int. Details: {e}")
        except TypeError as e: 
            self.plugin_log(f"Error initializing order_horizon: Invalid type for parameter {Config.ORDER_HORIZON}. Value: '{val}'. Details: {e}", _type="error")
            raise PluginException(f"Error initializing order_horizon: Invalid type for parameter {Config.ORDER_HORIZON}. Value: '{val}'. Details: {e}")
        except Exception as e: 
            self.plugin_log(f"Error initializing order_horizon: Invalid or missing parameter {Config.ORDER_HORIZON}. Details: {e}", _type="error")
            raise PluginException(f"Error initializing order_horizon: Invalid or missing parameter {Config.ORDER_HORIZON}. Details: {e}")

        try:
            val = self.parameters.get(Config.USE_ORDER_FORECAST_MAP)
            self.use_order_forecast_map = string_to_bool(val)
        except ValueError as e: 
            self.plugin_log(f"Error initializing use_order_forecast_map: Cannot convert value '{val}' to bool. Details: {e}", _type="error")
            raise PluginException(f"Error initializing use_order_forecast_map: Cannot convert value '{val}' to bool. Details: {e}")
        except TypeError as e: 
            self.plugin_log(f"Error initializing use_order_forecast_map: Invalid type for parameter {Config.USE_ORDER_FORECAST_MAP}. Value: '{val}'. Details: {e}", _type="error")
            raise PluginException(f"Error initializing use_order_forecast_map: Invalid type for parameter {Config.USE_ORDER_FORECAST_MAP}. Value: '{val}'. Details: {e}")
        except Exception as e:
            self.plugin_log(f"Error initializing use_order_forecast_map: Invalid or missing parameter {Config.USE_ORDER_FORECAST_MAP}. Details: {e}", _type="error")
            raise PluginException(f"Error initializing use_order_forecast_map: Invalid or missing parameter {Config.USE_ORDER_FORECAST_MAP}. Details: {e}")
        
        time_param_value = self.parameters.get(Config.TIME_COLUMN_KEY, "DefaultTimeValue")
        try:
            self.TIME = to_key_col(time_param_value)
        except (AttributeError, TypeError, IndexError) as e:
            self.plugin_log(f"Error processing time attribute '{Config.TIME_COLUMN_KEY}' (value: '{time_param_value}'): {e}", _type="error")
            raise PluginException(f"Error processing time attribute '{Config.TIME_COLUMN_KEY}' (value: '{time_param_value}'): {e}")
        except Exception as e: 
            self.plugin_log(f"Unexpected error processing time attribute '{Config.TIME_COLUMN_KEY}': {e}", _type="error")
            raise PluginException(f"Unexpected error processing time attribute '{Config.TIME_COLUMN_KEY}': {e}")
        self.plugin_log("DemandNetting initialized successfully.", _type="info")

    def plugin_log(self, message: str, _type: str = "info"):
        # Basic filtering for DEBUG messages can be done here or by the calling code
        # For now, just print with class name prefix.
        print(f"[{self.class_name}] LOG [{_type.upper()}]: {message}")


    def get_siblings(self, _orderValue, _map, hierarchyCol):
        if _orderValue not in _map:
            self.plugin_log(f"Value '{_orderValue}' not in _map for get_siblings", _type="warn")
            return [] 
        items_to_check = _map.get(_orderValue, {}).get('related_items', []) 
        result = []
        for i in items_to_check: # Loop: Minimal logging, only if issue.
            if i in _map:
                siblings = _map[i].get(hierarchyCol, [])
                result.extend(siblings)
            else:
                self.plugin_log(f"Item '{i}' referenced by '{_orderValue}' not found in _map for get_siblings", _type="warn")
        return result

    def form_consumption_tuples(self, items1, items2, items3, items4, const_val1, const_val2):
        count = len(items1) * len(items2) * len(items3) * len(items4)
        self.plugin_log(f"form_consumption_tuples would create {count} tuples (placeholder logic)", _type="debug")
        if count > 0: 
            return [(items1[0], items2[0], items3[0], items4[0], const_val1, const_val2)] 
        return []

    def create_tuples(self, _fTime, _orderValue, _orderData: dict, forecast_df: pd.DataFrame):
        consumption_tuple = [] 
        upward_item_values = _orderData.get("upward_items", [_orderValue]) 
        upward_location_values = _orderData.get("upward_locations", ["LOC1"])
        upward_customer_values = _orderData.get("upward_customers", ["CUST1"])
        
        if self.enable_time_hierarchy: 
            upward_time_values = _orderData.get("upward_times", [_fTime]) 
            self.plugin_log(f"Forming tuples - Hierarchy: I:{len(upward_item_values)}, L:{len(upward_location_values)}, S:{len(upward_customer_values)}, T:{len(upward_time_values)} for order {_orderData.get(self.DEMAND_ID, 'N/A')}", _type="debug")
            consumption_tuple.extend(
                self.form_consumption_tuples(upward_item_values, upward_location_values, upward_customer_values, upward_time_values, "HIERARCHY_CONST1", "HIERARCHY_CONST2")
            )
        else:
            backward_buckets_values = _orderData.get("backward_buckets", [_fTime]) 
            self.plugin_log(f"Forming tuples - Backward: I:{len(upward_item_values)}, L:{len(upward_location_values)}, S:{len(upward_customer_values)}, B:{len(backward_buckets_values)} for order {_orderData.get(self.DEMAND_ID, 'N/A')}", _type="debug")
            consumption_tuple.extend(
                self.form_consumption_tuples(upward_item_values, upward_location_values, upward_customer_values, backward_buckets_values, "BACKWARD_CONST1", "BACKWARD_CONST2")
            )
            forward_buckets_values = _orderData.get("forward_buckets", [_fTime]) 
            self.plugin_log(f"Forming tuples - Forward: I:{len(upward_item_values)}, L:{len(upward_location_values)}, S:{len(upward_customer_values)}, F:{len(forward_buckets_values)} for order {_orderData.get(self.DEMAND_ID, 'N/A')}", _type="debug")
            consumption_tuple.extend(
                 self.form_consumption_tuples(upward_item_values, upward_location_values, upward_customer_values, forward_buckets_values, "FORWARD_CONST1", "FORWARD_CONST2")
            )
        self.plugin_log(f"Generated {len(consumption_tuple)} consumption tuples for order {_orderData.get(self.DEMAND_ID, 'N/A')}", _type="debug")
        
        try:
            time_index = self.all_time_buckets.index(_fTime)
        except ValueError as e:
            self.plugin_log(f"Time value '{_fTime}' not found in all_time_buckets during tuple creation: {e}", _type="error")
            raise PluginException(f"Time value '{_fTime}' not found in all_time_buckets during tuple creation: {e}")
        
        siblings = self.get_siblings(_orderValue, self._map, self.HIERARCHY_COL) 
        created_tuple_info = (_fTime, _orderValue, time_index, len(siblings))
        self.plugin_log(f"Tuple info for order {_orderData.get(self.DEMAND_ID, 'N/A')}: {created_tuple_info}", _type="info")
        return consumption_tuple
    
    # --- Placeholder for other DemandNetting methods that would need logging review ---
    def preprocess_inputs(self, in_orders_df, forecast_df, some_dim_col_name="SomeDimCol"):
        self.plugin_log("Starting preprocess_inputs", _type="info")
        # ... (previous error handling logic) ...
        self.plugin_log("Finished preprocess_inputs", _type="info")

    def setup_orders(self, time_hierarchy_df=None):
        self.plugin_log("Starting setup_orders", _type="info")
        # ... (previous error handling logic) ...
        if time_hierarchy_df is None or time_hierarchy_df.empty:
            self.plugin_log("Time hierarchy data not provided or empty in setup_orders.", _type="warn")
        self.plugin_log("Finished setup_orders", _type="info")

    def setup_forcast(self):
        self.plugin_log("Starting setup_forcast", _type="info")
        # ... (previous error handling logic) ...
        self.plugin_log("Finished setup_forcast", _type="info")

    def customTimeDelta(self, timeBucket, offset):
        self.plugin_log(f"Calculating customTimeDelta for {timeBucket} with offset {offset}", _type="debug")
        # ... (previous error handling logic) ...
        return "CalculatedTimeValue" # Placeholder

    def consume_from_forecast_index(self, _orderIndex, forecastIn):
        self.plugin_log(f"Consuming from forecast index: Order '{_orderIndex}', Forecast '{forecastIn}'", _type="debug")
        # ... (previous error handling logic) ...
        return 0 # Placeholder consumed amount

    def consume_from_forecast_index_with_pegging(self, _orderIndex, forecastIn, pegging_map):
        self.plugin_log(f"Consuming (pegging) from forecast index: Order '{_orderIndex}', Forecast '{forecastIn}'", _type="debug")
        # ... (previous error handling logic) ...
        return 0 # Placeholder consumed amount
    
    def get_demand_types(self, fs_order: pd.DataFrame, os_order: pd.DataFrame):
        self.plugin_log("Starting get_demand_types", _type="info")
        if fs_order.empty: self.plugin_log("fs_order DataFrame is empty in get_demand_types.", _type="warn")
        if os_order.empty: self.plugin_log("os_order DataFrame is empty in get_demand_types.", _type="warn")
        # ... (previous error handling logic, ensure any PluginExceptions are preceded by an error log) ...
        self.plugin_log("Finished get_demand_types", _type="info")
        return fs_order # Placeholder

    def run_netting(self):
        self.plugin_log("Starting run_netting", _type="info")
        # ... (previous error handling logic) ...
        self.plugin_log("Finished run_netting", _type="info")

    def run_netting_for_rtf(self):
        self.plugin_log("Starting run_netting_for_rtf", _type="info")
        # ... (previous error handling logic) ...
        self.plugin_log("Finished run_netting_for_rtf", _type="info")


# --- SkipNetting Class ---
class SkipNetting:
    def __init__(self, parameters):
        self.class_name = self.__class__.__name__
        self.parameters = parameters
        self.in_orders = pd.DataFrame()
        self.in_forecast = pd.DataFrame()
        self.in_forecastStreamParameters = pd.DataFrame()
        self.DEMAND_TYPE = self.parameters.get(Config.DEMAND_TYPE, "DefaultDemandTypeCol")
        self.order_qty = self.parameters.get(Config.ORDER_QTY_COL, "DefaultOrderQtyCol")
        self.fs_map = {}
        self.plugin_log("SkipNetting initialized.", _type="info")

    def plugin_log(self, message: str, _type: str = "info"):
        print(f"[{self.class_name}] LOG [{_type.upper()}]: {message}")

    def pre_proc(self, in_orders: pd.DataFrame, in_forecast: pd.DataFrame):
        self.plugin_log("Starting SkipNetting pre_proc", _type="info")
        self.in_orders = pd.DataFrame() if in_orders is None else in_orders.copy()
        self.in_forecast = pd.DataFrame() if in_forecast is None else in_forecast.copy()
        try:
            if not self.in_orders.empty:
                self.in_orders['SomeOrderCol'] = self.in_orders['SomeOrderCol'].astype(str)
            else:
                self.plugin_log("in_orders is empty during SkipNetting pre_proc.", _type="warn")
        except (TypeError, AttributeError) as e:
            self.plugin_log(f"Error during type casting in SkipNetting in_orders: {e}", _type="error")
            raise PluginException(f"Error during type casting in SkipNetting in_orders: {e}")
        except KeyError as e:
            self.plugin_log(f"Missing expected column 'SomeOrderCol' in SkipNetting in_orders: {e}", _type="error")
            raise PluginException(f"Missing expected column 'SomeOrderCol' in SkipNetting in_orders: {e}")
        
        try:
            if not self.in_forecast.empty:
                self.in_forecast['SomeForecastCol'] = self.in_forecast['SomeForecastCol'].astype(float)
            else:
                self.plugin_log("in_forecast is empty during SkipNetting pre_proc.", _type="warn")
        except (TypeError, AttributeError) as e:
            self.plugin_log(f"Error during type casting in SkipNetting in_forecast: {e}", _type="error")
            raise PluginException(f"Error during type casting in SkipNetting in_forecast: {e}")
        except KeyError as e:
            self.plugin_log(f"Missing expected column 'SomeForecastCol' in SkipNetting in_forecast: {e}", _type="error")
            raise PluginException(f"Missing expected column 'SomeForecastCol' in SkipNetting in_forecast: {e}")
        self.plugin_log("Finished SkipNetting pre_proc", _type="info")

    def make_fs_map(self):
        self.plugin_log("Starting SkipNetting make_fs_map", _type="info")
        if self.in_forecastStreamParameters.empty:
            self.plugin_log("Forecast stream parameters are empty, fs_map will be empty.", _type="warn")
            self.fs_map = {}
            return
        def some_processing_func(row): return str(row.get("param1", "")) + "_" + str(row.get("param2", ""))
        try:
            self.fs_map = self.in_forecastStreamParameters.apply(
                lambda row: (row['key_col'], some_processing_func(row)), axis=1
            ).to_dict()
        except KeyError as e:
            self.plugin_log(f"Missing expected column in forecast stream parameters for make_fs_map: {e}", _type="error")
            raise PluginException(f"Missing expected column in forecast stream parameters for make_fs_map: {e}")
        except Exception as e:
            self.plugin_log(f"Error processing forecast stream parameters in SkipNetting: {e}", _type="error")
            raise PluginException(f"Error processing forecast stream parameters in SkipNetting: {e}")
        self.plugin_log(f"Finished SkipNetting make_fs_map, fs_map size: {len(self.fs_map)}", _type="info")

    def run_skip_netting(self):
        self.plugin_log("Starting run_skip_netting", _type="info")
        self.make_fs_map()
        try:
            if not self.in_orders.empty:
                self.in_orders[self.DEMAND_TYPE] = "Orders"
                qty_sum = self.in_orders[self.order_qty].sum()
                self.plugin_log(f"Sum of order_qty in SkipNetting: {qty_sum}", _type="info")
            else:
                self.plugin_log("in_orders is empty in run_skip_netting.", _type="warn")
        except KeyError as e:
            self.plugin_log(f"Missing expected column in run_skip_netting: {e}", _type="error")
            raise PluginException(f"Missing expected column in SkipNetting: {e}")
        self.plugin_log("Finished run_skip_netting", _type="info")
        return self.in_orders


# --- Profiling Class ---
class Profiling:
    def __init__(self, parameters):
        self.class_name = self.__class__.__name__
        self.parameters = parameters
        self.in_netted_forecast = pd.DataFrame()
        self.in_basis = pd.DataFrame()
        self.in_telescopic = pd.DataFrame()
        self.final_time_df = pd.DataFrame() 

        self.basis_col = self.parameters.get(Config.BASIS_COL, "DefaultBasisCol")
        self.telescopic_col = self.parameters.get(Config.TELESCOPIC_COL, "DefaultTelescopicCol")
        self.profile_method_col = self.parameters.get(Config.PROFILE_METHOD_COL, "DefaultProfileMethodCol")
        self.order_due_date = self.parameters.get(Config.ORDER_DUE_DATE_COL, "DefaultOrderDueDateCol")
        self.final_time_attribute = self.parameters.get(Config.FINAL_TIME_ATTR_COL, "DefaultFinalTimeAttrCol")
        self.netted_demand_qty = self.parameters.get(Config.NETTED_DEMAND_QTY_COL, "NettedDemandQty")
        self.basis_spread_method = self.parameters.get("BasisSpreadMethod", "Distribute")
        self.plugin_log("Profiling initialized.", _type="info")

    def plugin_log(self, message: str, _type: str = "info"):
        print(f"[{self.class_name}] LOG [{_type.upper()}]: {message}")

    def pre_proc(self, in_netted_forecast: pd.DataFrame, in_basis: pd.DataFrame, in_telescopic: pd.DataFrame):
        self.plugin_log("Starting Profiling pre_proc", _type="info")
        self.in_netted_forecast = pd.DataFrame() if in_netted_forecast is None else in_netted_forecast.copy()
        self.in_basis = pd.DataFrame() if in_basis is None else in_basis.copy()
        self.in_telescopic = pd.DataFrame() if in_telescopic is None else in_telescopic.copy()

        try:
            if not self.in_netted_forecast.empty:
                self.in_netted_forecast['NettedQty'] = self.in_netted_forecast['NettedQty'].astype(float)
            else:
                self.plugin_log("in_netted_forecast is empty in Profiling pre_proc.", _type="warn")
        except (TypeError, AttributeError) as e:
            self.plugin_log(f"Error during type casting in Profiling pre_proc for in_netted_forecast: {e}", _type="error")
            raise PluginException(f"Error during type casting in Profiling pre_proc for in_netted_forecast: {e}")
        except KeyError as e:
            self.plugin_log(f"Missing 'NettedQty' column in Profiling pre_proc for in_netted_forecast: {e}", _type="error")
            raise PluginException(f"Missing 'NettedQty' column in Profiling pre_proc for in_netted_forecast: {e}")
        # ... (similar error handling for in_basis and in_telescopic from Turn 13, with added logging) ...
        self.plugin_log("Finished Profiling pre_proc", _type="info")

    def spread_forecast_to_partial_week(self):
        self.plugin_log("Starting spread_forecast_to_partial_week", _type="info")
        if self.in_netted_forecast.empty or 'total' not in self.in_netted_forecast.columns or self.basis_col not in self.in_netted_forecast.columns:
            self.plugin_log("Not enough data for spread_forecast_to_partial_week, skipping.", _type="warn")
            return
        try:
            if not pd.api.types.is_numeric_dtype(self.in_netted_forecast['total']):
                self.in_netted_forecast['total'] = pd.to_numeric(self.in_netted_forecast['total'], errors='coerce').fillna(0)
            if (self.in_netted_forecast['total'] == 0).any():
                self.plugin_log("Division by zero: 'total' is zero in spread_forecast_to_partial_week.", _type="error")
                raise PluginException("Division by zero: 'total' is zero in spread_forecast_to_partial_week for at least one row.")
            self.in_netted_forecast['ratio'] = self.in_netted_forecast[self.basis_col] / self.in_netted_forecast['total']
        except KeyError as e:
             self.plugin_log(f"Missing column for spread calculation: {e}", _type="error")
             raise PluginException(f"Missing column for spread calculation: {e}")
        except TypeError as e:
             self.plugin_log(f"Type error during spread calculation: {e}", _type="error")
             raise PluginException(f"Type error during spread calculation: {e}")
        except Exception as e:
             self.plugin_log(f"Unexpected error in spread_forecast_to_partial_week: {e}", _type="error")
             raise PluginException(f"Unexpected error in spread_forecast_to_partial_week: {e}")
        self.plugin_log("Finished spread_forecast_to_partial_week", _type="info")

    def apply_norm(self, group: pd.DataFrame):
        # self.plugin_log(f"Applying norm for group (size: {len(group)})", _type="debug") # Example of loop-context log
        if group.empty:
            self.plugin_log("Empty group passed to apply_norm", _type="warn")
            return group
        try:
            group.sort_values(by=["ratio"], inplace=True, ignore_index=True)
            if "forecast_to_distribute" not in group.columns or "ratio" not in group.columns:
                 self.plugin_log("Missing 'forecast_to_distribute' or 'ratio' column in apply_norm.", _type="error")
                 raise KeyError("Missing 'forecast_to_distribute' or 'ratio' column for apply_norm 'Distribute' logic.")
            total_to_distribute = group["forecast_to_distribute"].iloc[0]
            ratios = group["ratio"].to_numpy(dtype=float)
            distributed_quantities = np.zeros(len(group), dtype=int)
            sum_until_now = 0
            ratio_of_rem_rows = np.sum(ratios)
            if abs(ratio_of_rem_rows) < 1e-9 and abs(total_to_distribute) > 1e-9 :
                self.plugin_log(f"Cannot distribute {total_to_distribute}: sum of ratios is zero in apply_norm.", _type="error")
                raise PluginException(f"Cannot distribute {total_to_distribute}: sum of ratios is zero in apply_norm for group.")
            
            # Loop: Logging inside should be DEBUG if verbose, or WARN/ERROR for specific issues.
            for i in range(len(ratios)):
                current_ratio = ratios[i]
                if abs(ratio_of_rem_rows) < 1e-9: 
                    if abs(total_to_distribute - sum_until_now) > 1e-9:
                         self.plugin_log(f"Zero ratio_of_rem_rows with remaining amount in apply_norm for group. Row {i}", _type="warn")
                    dist_int = 0 
                else:
                    dist_int = floor(((total_to_distribute - sum_until_now) * current_ratio) / ratio_of_rem_rows)
                distributed_quantities[i] = dist_int
                sum_until_now += dist_int
                ratio_of_rem_rows -= current_ratio
            group[self.netted_demand_qty] = distributed_quantities
        except KeyError as e:
            self.plugin_log(f"Missing column in apply_norm: {e}", _type="error")
            raise PluginException(f"Missing column in apply_norm: {e}")
        except IndexError as e: 
            self.plugin_log(f"Empty group or issue with data structure passed to apply_norm: {e}", _type="error")
            raise PluginException(f"Empty group or issue with data structure passed to apply_norm: {e}")
        except Exception as e:
            self.plugin_log(f"Error in apply_norm ('Distribute' method): {e}", _type="error")
            raise PluginException(f"Error in apply_norm ('Distribute' method): {e}")
        return group

    def run_profiling(self):
        self.plugin_log("Starting run_profiling", _type="info")
        # Merges
        # ... (Error handling from Turn 13 for merges with added plugin_log for errors) ...
        if self.in_netted_forecast.empty or self.in_basis.empty:
            self.plugin_log("Skipping basis merge due to empty DF(s) in run_profiling.", _type="warn")
        if self.in_netted_forecast.empty or self.in_telescopic.empty:
            self.plugin_log("Skipping telescopic merge due to empty DF(s) in run_profiling.", _type="warn")

        # Groupby operations
        if not self.in_netted_forecast.empty and self.basis_spread_method == "Distribute":
            # ... (Error handling from Turn 15 for groupby and apply_norm call, with added plugin_log for errors) ...
            if "forecast_to_distribute" not in self.in_netted_forecast.columns:
                self.plugin_log("Creating dummy 'forecast_to_distribute' for apply_norm in run_profiling", _type="warn")
                self.in_netted_forecast["forecast_to_distribute"] = 100 
            if "ratio" not in self.in_netted_forecast.columns:
                self.plugin_log("Creating dummy 'ratio' for apply_norm in run_profiling", _type="warn")
                self.in_netted_forecast["ratio"] = 0.1 
            # ... (apply call) ...
        elif self.in_netted_forecast.empty:
            self.plugin_log("in_netted_forecast is empty, skipping main profiling logic.", _type="warn")

        self.plugin_log("Finished run_profiling", _type="info")
        return self.in_netted_forecast

    def profile_demand(self, group):
        self.plugin_log(f"Starting profile_demand for group (size: {len(group)})", _type="debug")
        order_due_date_val = "N/A" # For logging in case of error before assignment
        try:
            order_due_date_val = group[self.order_due_date].iloc[0]
            pd.to_datetime(order_due_date_val)
            self.final_time_df[self.final_time_attribute].iloc[0]
        except ValueError as e:
            self.plugin_log(f"Error converting date in profile_demand (value: {order_due_date_val}): {e}", _type="error")
            raise PluginException(f"Error converting date in profile_demand (value: {order_due_date_val}): {e}")
        except (KeyError, IndexError) as e:
            self.plugin_log(f"Error accessing data in profile_demand: {e}", _type="error")
            raise PluginException(f"Error accessing data in profile_demand: {e}")
        except Exception as e:
            self.plugin_log(f"Unexpected error in profile_demand: {e}", _type="error")
            raise PluginException(f"Unexpected error in profile_demand: {e}")
        self.plugin_log(f"Finished profile_demand for group (size: {len(group)})", _type="debug")
        return group


# --- Example Usage ---
if __name__ == '__main__':
    params = {
        Config.DEMAND_ID_COL: "OrderNumber",
        Config.NETTED_DEMAND_QTY_COL: "FinalQty",
        "BasisSpreadMethod": "Distribute" 
    }
    print("--- DemandNetting Logging Example ---")
    dn = DemandNetting(params)
    dn.all_time_buckets = ["W1", "W2", "W3"] 
    dn._map = {"OrderA": {"related_items": ["Item1"]}, "Item1": {"ProductGroup": ["PG1_Child"]}}
    sample_order_data = {dn.DEMAND_ID: "SO123", "upward_items": ["ItemX"], "upward_locations": ["LocA"], "upward_customers": ["CustX"]}
    dn.create_tuples("W1", "OrderA", sample_order_data, pd.DataFrame())
    dn.create_tuples("W4", "OrderA", sample_order_data, pd.DataFrame()) # Expected error log

    print("\n--- SkipNetting Logging Example ---")
    sn = SkipNetting(params)
    sn.pre_proc(None, None) # Expected warn logs
    sn.run_skip_netting()

    print("\n--- Profiling Logging Example ---")
    prof = Profiling(params)
    prof.pre_proc(None, None, None) # Expected warn logs
    prof.in_netted_forecast = pd.DataFrame({'total':[0], prof.basis_col: [10]})
    prof.spread_forecast_to_partial_week() # Expected error log
    
    sample_group = pd.DataFrame({'ratio': [0.2, 0.3, 0.5], 'forecast_to_distribute': [100, 100, 100], prof.netted_demand_qty: [0, 0, 0]})
    prof.apply_norm(sample_group)
    prof.run_profiling() # Expected warn logs for dummy data creation

    print("\nLogging refinement complete (simulated).")
