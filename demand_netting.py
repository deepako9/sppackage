"""
    Class: DemandNetting
    Version : v24.4.0
    Maintained by : pmm_algocoe@o9solutions.com

    Modification History:
        v24.1
            - Codelib 2 Support
        v24.2
            - Unique value fix. (https://o9git.visualstudio.com/RefDev/_workitems/edit/332112)
        v24.3
            - NEW & UNF value incorrect. (https://o9git.visualstudio.com/RefDev/_workitems/edit/364296)
        v24.4
            - Add parameterization for Netting.
            - Add Multistream Netting.
        v24.5
            - Added Profiling & Skip Netting
            - Fix multistream calculation for consumption by multiple forecasts.
"""

from pandas import DataFrame, to_datetime, merge, Series, concat, isna
from collections import defaultdict
from numpy import arange, vectorize, where, ceil, floor
from time import time
from itertools import permutations
import datetime
from pandas.tseries.offsets import DateOffset


class PluginException(Exception):
    pass


class Config:
    O9_PARAMETER: str = "o9 Parameter.[Parameter Name]"
    O9_PARAMETER_VALUE: str = "Core Create Network Parameter Value"
    DN_USE_MULTI_STREAM: str = "Use Multi Stream Netting"
    DN_ITEM_ATTR: str = "Netting Item Attribute"
    DN_LOCATION_ATTR: str = "Netting Location Attribute"
    DN_CUSTOMER_ATTR: str = "Netting Customer Attribute"
    DN_TIME_ATTR: str = "Netting Time Attribute"
    DN_TELESCOPIC_TIME_ATTR: str = "Netting Output Time Attribute"
    DN_OUT_FINAL_TIME_ATTR: str = "Netting Final Output Time Attribute"
    DN_ORDER_QTY: str = "Netting Order Quantity"
    DN_OPEN_ORDER: str = "Netting Open Order Quantity"
    DN_ORDER_PRIORITY: str = "Netting Order Priority"
    DN_ORDER_TYPE: str = "Netting Order Type"
    DN_BACKWARD_BUCKETS: str = "Netting Order Backward Buckets"
    DN_FORWARD_BUCKETS: str = "Netting Order Forward Buckets"
    DN_UPWARD_ITEM: str = "Netting Order Item Upwards Buckets"
    DN_UPWARD_LOCATION: str = "Netting Order Location Upwards Bucket"
    DN_UPWARD_CUSTOMER: str = "Netting Order Customer Upwards Bucket"
    DN_UPWARD_TIME: str = "Netting Order Time Upwards Bucket"
    DN_EXCLUDE_NETTING: str = "Netting Exclude Order from Netting"
    DN_EXCLUDE_PLANNING: str = "Netting Exclude Order from Planning"
    DN_FORECAST_QTY: str = "Netting Forecast Measure Name"
    DN_F_BACKWARD_BUCKETS: str = "Netting Forecast Backward Buckets"
    DN_F_FORWARD_BUCKETS: str = "Netting Forecast Forward Buckets"
    DN_F_UPWARD_ITEM: str = "Netting Forecast Item Upwards Buckets"
    DN_F_UPWARD_LOCATION: str = "Netting Forecast Location Upwards Bucket"
    DN_F_UPWARD_CUSTOMER: str = "Netting Forecast Customer Upwards Bucket"
    DN_F_UPWARD_TIME: str = "Netting Forecast Time Upwards Bucket"
    DN_F_EXCLUDE_NETTING: str = "Netting Exclude Forecast from Netting"
    DN_F_EXCLUDE_PLANNING: str = "Netting Exclude Forecast from Planning"
    DN_RTF_QTY: str = "Netting RTF Measure Name"
    DN_OUT_QTY: str = "Netting Output Measure Name"
    DN_SKIP_NETTING: str = "Skip Netting Mode"
    DN_USE_AGGREGATE: str = "Use Aggregate Netting"
    DN_DISABLE_BUCKETS: str = "Disable Buckets"
    DN_SPLIT_DEMAND: str = "Split Demand Types"
    DN_USE_MAPPING: str = "Use Mapping Graph"
    DN_PEGGING: str = "Netting Pegging"
    DN_TIME_HIERARCHY: str = "Use Time Hierachical instead of Backward Forward"
    DN_CONSUMPTION_ORDER: str = "Backward Forward Consumption Order"
    DN_H_CONSUMPTION_ORDER: str = "Hierarchical Consumption Order"
    DN_AGGREGATE_LEVELS: str = "Netting Aggregate Levels"
    DN_SELF_AGGREGATE: str = "Consume Self Before consuming at Aggregate Levels"
    DN_OUT_AGGREGATE_GRAIN: str = "Outputs at Aggregated Netting level"
    DN_OUT_PROFILED_BUCKET: str = "Netting Profiling Spread method"
    DN_PROFILED_BASIS: str = "Netting Profiling Basis Measure Name"
    DN_PROFILED_BASIS_SPREAD_METHOD: str = "Netting Assortment to Profiled Output Buckets"
    DN_PROFILED_ASSORTMENT_BASIS: str = (
        "Netting Profiling Assortment Basis Measure Name"
    )
    DN_ORDER_AND_FORECAST_PRIORITY_BY_WEEK: str = ""
    DN_ENABLE_BACKWARD_BEFORE_CURRENT: str = "Reverse Time Backward Consumption Order"
    DN_ORDER_HORIZON: str = "Order Horizon"
    DN_ORDER_DUE_DATE: str = "Netting Order Due Date"
    # Default Values
    USE_MULTI_STREAM: str = "0"
    USE_MAPPING: str = "0"
    PEGGING: str = "0"
    SPLIT_DEMAND: str = "1"
    DISABLE_BUCKETS: str = "0"
    USE_AGGREGATE: str = "0"
    SKIP_NETTING: str = "0"
    OUT_AGGREGATE_GRAIN: str = "0"
    ENABLE_BACKWARD_BEFORE_CURRENT: str = "0"
    VERSION: str = "Version.[Version Name]"
    DEMAND_TYPE: str = "Demand Type.[Demand Type]"
    DEMAND_ID: str = "Demand.[DemandID]"
    ITEM: str = "Item.[Item]"
    LOCATION: str = "Location.[Location]"
    CUSTOMER: str = "Sales Domain.[Customer Group]"
    WEEK: str = "Time.[Week]"
    ORDER_QTY: str = "Order Quantity"
    FORECAST_QTY: str = "Base Forecast Quantity"
    RTF_QTY: str = "RTF"
    OPEN_ORDER_QTY: str = "Open Order Quantity"
    ORDER_PRIORITY: str = "Order Priority"
    # PARAMS / BUCKETS
    BACKWARD_BUCKETS: str = "Netting Backward Buckets Order"
    FORWARD_BUCKETS: str = "Netting Forward Buckets Order"
    UPWARD_ITEM: str = "Netting Item Upward Levels Order"
    UPWARD_LOCATION: str = "Netting Location Upward Levels Order"
    UPWARD_CUSTOMER: str = "Netting Sales Domain Upward Levels Order"
    UPWARD_TIME: str = "Netting Time Upward Levels Order"
    F_BACKWARD_BUCKETS: str = "Netting Backward Buckets Forecast"
    F_FORWARD_BUCKETS: str = "Netting Forward Buckets Forecast"
    F_UPWARD_ITEM: str = "Netting Item Upward Levels Forecast"
    F_UPWARD_LOCATION: str = "Netting Location Upward Levels Forecast"
    F_UPWARD_CUSTOMER: str = "Netting Sales Domain Upward Levels Forecast"
    F_UPWARD_TIME: str = "Netting Time Upward Levels Forecast"
    EXCLUDE_NETTING: str = "Exclude from Netting Order"
    EXCLUDE_PLANNING: str = "Exclude from Planning Order"
    F_EXCLUDE_NETTING: str = "Exclude from Netting Forecast"
    F_EXCLUDE_PLANNING: str = "Exclude from Planning Forecast"
    COM_ORDER: str = "COM_ORDER"
    NEW_ORDER: str = "NEW_ORDER"
    UNF_ORDER: str = "UNF_ORDER"
    COM_FCST: str = "COM_FCST"
    NEW_FCST: str = "NEW_FCST"
    FORECAST_DEMAND_ID: str = "NetBaseForecast"
    ORDER_TYPE: str = "Order Type"

    ORDER_HORIZON: str = "-1"
    PROFILED_ASSORTMENT_BASIS = "W Netting Split Applicable Weeks"
    PROFILED_BASIS = "W Netting Split Intermediate"
    PROFILED_BASIS_SPREAD_METHOD = "Distribute"
    OUT_QTY = "Netted Demand Quantity"
    OUT_PROFILED_BUCKET = "First Bucket"
    OUT_FINAL_TIME_ATTR = "Time.[Week]"
    ORDER_DUE_DATE = "Order Due Date"


class DemandNetting:
    """Demand Netting Logic"""

    def __init__(
            self,
            in_orders: DataFrame,
            in_forecasts: DataFrame,
            in_RTFs: DataFrame,
            master_item: DataFrame,
            master_location: DataFrame,
            master_salesDomain: DataFrame,
            master_time: DataFrame,
            in_telescopic: DataFrame,
            in_orderForecastMapGraph: DataFrame,
            in_orderStreamParameters: DataFrame,
            in_forecastStreamParameters: DataFrame,
            in_pastOrderDate: DataFrame,
            in_parameters: dict,
            in_basis: DataFrame,
            logger,
    ):
        self.startTime = time()
        self.class_name: str = __name__
        self.class_version: str = "v24.5"
        self.logger = logger
        # Inputs
        self.in_orders: DataFrame = in_orders
        self.in_forecasts: DataFrame = in_forecasts
        self.in_RTFs: DataFrame = in_RTFs
        self.master_item: DataFrame = master_item
        self.master_location: DataFrame = master_location
        self.master_customer: DataFrame = master_salesDomain
        self.master_time: DataFrame = master_time
        self.in_telescopic: DataFrame = in_telescopic
        self.in_orderStreamParameters: DataFrame = in_orderStreamParameters
        self.in_orderForecastMapGraph: DataFrame = in_orderForecastMapGraph
        self.in_forecastStreamParameters: DataFrame = in_forecastStreamParameters
        self.in_pastOrderDate: DataFrame = in_pastOrderDate
        self.past_orders: DataFrame = DataFrame()
        self.in_basis: DataFrame = in_basis

        self.in_parameters = in_parameters

        self.parameters: dict = in_parameters

        def string_to_bool(s: str):
            if s.lower() == "true" or s == "1":
                return True
            return False

        def string_to_int(s: str):
            try:
                temp = int(s)
                return temp
            except Exception as e:
                return -1

        # Variables
        self.order_horizon: int = string_to_int(
            str(self.parameters.get(Config.DN_ORDER_HORIZON, Config.ORDER_HORIZON))
        )
        self.use_order_forecast_map: bool = string_to_bool(
            str(self.parameters.get(Config.DN_USE_MAPPING, Config.USE_MAPPING))
        )
        self.use_aggregate: bool = string_to_bool(
            str(self.parameters.get(Config.DN_USE_AGGREGATE, Config.USE_AGGREGATE))
        )
        self.use_multi_stream: bool = string_to_bool(
            str(
                self.parameters.get(Config.DN_USE_MULTI_STREAM, Config.USE_MULTI_STREAM)
            )
        )
        self.pegging_flag: bool = string_to_bool(
            str(self.parameters.get(Config.DN_PEGGING, Config.PEGGING))
        )
        self.prioritize_order_forecast_by_time: bool = string_to_bool(
            self.parameters.get(Config.DN_PEGGING, "0")
        )
        self.split_demand_type: bool = string_to_bool(
            str(self.parameters.get(Config.DN_SPLIT_DEMAND, Config.SPLIT_DEMAND))
        )
        self.enable_time_hierarchy: bool = string_to_bool(
            self.parameters.get(Config.DN_TIME_HIERARCHY, "0")
        )
        self.no_bucket_flag: bool = string_to_bool(
            self.parameters.get(Config.DN_DISABLE_BUCKETS, "0")
        )
        self.prioritize_order_forecast_by_time_flag: bool = string_to_bool(
            self.parameters.get(Config.DN_ORDER_AND_FORECAST_PRIORITY_BY_WEEK, "0")
        )
        self.skip_netting: bool = string_to_bool(
            str(self.parameters.get(Config.DN_SKIP_NETTING, Config.SKIP_NETTING))
        )
        self.enable_backward_before_current: bool = string_to_bool(
            self.parameters.get(
                Config.DN_ENABLE_BACKWARD_BEFORE_CURRENT,
                Config.ENABLE_BACKWARD_BEFORE_CURRENT,
            )
        )

        self.output_at_aggregated_level: bool = string_to_bool(
            str(
                self.parameters.get(
                    Config.DN_OUT_AGGREGATE_GRAIN, Config.OUT_AGGREGATE_GRAIN
                )
            )
        )

        self.VERSION: str = Config.VERSION
        self.DEMAND_TYPE: str = Config.DEMAND_TYPE
        self.DEMAND_ID: str = Config.DEMAND_ID
        self.ITEM: str = self.parameters.get(Config.DN_ITEM_ATTR, Config.ITEM)
        self.LOCATION: str = self.parameters.get(
            Config.DN_LOCATION_ATTR, Config.LOCATION
        )
        self.CUSTOMER: str = self.parameters.get(
            Config.DN_CUSTOMER_ATTR, Config.CUSTOMER
        )

        def to_key_col(s: str):
            s = str(s)
            s_i = s.find("[")
            e_i = s.find("]")
            content = s[s_i + 1: e_i]
            content = content.replace(" ", "") + "Key"
            return f"Time.[{content}]"

        self.TIME: str = self.parameters.get(Config.DN_TIME_ATTR, Config.WEEK)
        self.TIME = to_key_col(self.TIME)

        self.order_qty: str = self.parameters.get(Config.DN_ORDER_QTY, Config.ORDER_QTY)
        self.forecast_qty: str = self.parameters.get(
            Config.DN_FORECAST_QTY, Config.FORECAST_QTY
        )
        self.rtf_qty: str = self.parameters.get(Config.DN_RTF_QTY, Config.RTF_QTY)
        self.open_order_qty: str = self.parameters.get(
            Config.DN_OPEN_ORDER, Config.OPEN_ORDER_QTY
        )
        self.order_priority: str = self.parameters.get(
            Config.DN_ORDER_PRIORITY, Config.ORDER_PRIORITY
        )
        self.order_type: str = Config.ORDER_TYPE

        self.netted_demand_qty: str = self.parameters.get(
            Config.DN_OUT_QTY, Config.OUT_QTY
        )
        self.order_due_date: str = self.parameters.get(
            Config.DN_ORDER_DUE_DATE, Config.ORDER_DUE_DATE
        )

        self.TelescopicHeader: str = self.parameters.get(
            Config.DN_TELESCOPIC_TIME_ATTR, self.TIME
        )
        # PARAMS / BUCKETS
        self.BACKWARD_BUCKETS: str = self.parameters.get(
            Config.DN_BACKWARD_BUCKETS, Config.BACKWARD_BUCKETS
        )
        self.FORWARD_BUCKETS: str = self.parameters.get(
            Config.DN_FORWARD_BUCKETS, Config.FORWARD_BUCKETS
        )
        self.F_BACKWARD_BUCKETS: str = self.parameters.get(
            Config.DN_F_BACKWARD_BUCKETS, Config.F_BACKWARD_BUCKETS
        )
        self.F_FORWARD_BUCKETS: str = self.parameters.get(
            Config.DN_F_FORWARD_BUCKETS, Config.F_FORWARD_BUCKETS
        )
        self.UPWARD_ITEM: str = self.parameters.get(
            Config.DN_UPWARD_ITEM, Config.UPWARD_ITEM
        )
        self.F_UPWARD_ITEM: str = self.parameters.get(
            Config.DN_F_UPWARD_ITEM, Config.F_UPWARD_ITEM
        )
        self.UPWARD_LOCATION: str = self.parameters.get(
            Config.DN_UPWARD_LOCATION, Config.UPWARD_LOCATION
        )
        self.F_UPWARD_LOCATION: str = self.parameters.get(
            Config.DN_F_UPWARD_LOCATION, Config.F_UPWARD_LOCATION
        )
        self.UPWARD_CUSTOMER: str = self.parameters.get(
            Config.DN_UPWARD_CUSTOMER, Config.UPWARD_CUSTOMER
        )
        self.F_UPWARD_CUSTOMER: str = self.parameters.get(
            Config.DN_F_UPWARD_CUSTOMER, Config.F_UPWARD_CUSTOMER
        )
        self.UPWARD_TIME: str = self.parameters.get(
            Config.DN_UPWARD_TIME, Config.UPWARD_TIME
        )
        self.F_UPWARD_TIME: str = self.parameters.get(
            Config.DN_F_UPWARD_TIME, Config.F_UPWARD_TIME
        )
        self.EXCLUDE_NETTING: str = self.parameters.get(
            Config.DN_EXCLUDE_NETTING, Config.EXCLUDE_NETTING
        )
        self.EXCLUDE_PLANNING: str = self.parameters.get(
            Config.DN_EXCLUDE_PLANNING, Config.EXCLUDE_PLANNING
        )
        self.F_EXCLUDE_NETTING: str = self.parameters.get(
            Config.DN_F_EXCLUDE_NETTING, Config.F_EXCLUDE_NETTING
        )
        self.F_EXCLUDE_PLANNING: str = self.parameters.get(
            Config.DN_F_EXCLUDE_PLANNING, Config.F_EXCLUDE_PLANNING
        )
        self.ConsumeOnNativeBeforeAggregation: bool = bool(
            self.parameters.get(Config.DN_SELF_AGGREGATE, False)
        )

        # Local Variable
        self.curVersion: str = ""
        self.time_key: str = "time_key"
        self.time_priority: str = "time_priority"
        # Forecast
        self.f_item: str = self.ITEM
        self.f_location: str = self.LOCATION
        self.f_customer: str = self.CUSTOMER
        self.f_time: str = self.parameters.get(Config.DN_TIME_ATTR, Config.WEEK)
        self.f_time = to_key_col(self.f_time)
        # Graph
        self.from_item: str = ""
        self.from_location: str = ""
        self.from_customer: str = ""
        self.to_item: str = ""
        self.to_location: str = ""
        self.to_customer: str = ""
        self.graph_priority: str = (
            "005.001 Common Netting Association.[Forecast Order Priority]"
        )
        self.graph_rtf_association: str = (
            "005.001 Common Netting Association.[Forecast Order RTF Association]"
        )
        self.graph_forecast_association: str = (
            "005.001 Common Netting Association.[Forecast Order Association]"
        )
        # Stream
        self.os_cons_seq: str = "Forecast Consumption Sequence"
        self.os_stream: str = "Order Stream"
        self.os_forecast: str = "Forecast Stream Order"
        self.os_is_rtf: str = "RTF Netting Order Stream"
        self.os_com_order: str = "Committed Order Demand Type"
        self.os_new_order: str = "New Order Demand Type"
        self.os_unf_order: str = "Unforecasted Order Demand Type"
        self.os_past_order_dt: str = "Past Order Demand Type"
        self.fs_seq: str = "Forecast Netting Sequence"
        self.fs_stream: str = "Forecast Stream"
        self.fs_demand_id: str = "Forecast Demand ID"
        self.fs_is_rtf: str = "RTF Netting Forecast Stream"
        self.fs_com_forecast: str = "Committed Forecast Demand Type"
        self.fs_new_forecast: str = "New Forecast Demand Type"
        self.os_order_consumed: str = "order_consumed"
        self.os_order_remaining: str = "order_remaining"
        self.fs_forecast_consumed: str = "forecast_consumed"
        self.fs_forecast_remaining: str = "forecast_remaining"
        # CREATE PEGGING GRAPH HEADERS
        self.peg_seq: str = "FROM.[Sequence ID].[Sequence ID]"
        self.peg_from_demand_id: str = f'FROM.[{"].".join(self.DEMAND_ID.split("."))}'
        self.peg_from_item: str = f'FROM.[{"].".join(self.ITEM.split("."))}'
        self.peg_from_location: str = f'FROM.[{"].".join(self.LOCATION.split("."))}'
        self.peg_from_customer: str = f'FROM.[{"].".join(self.CUSTOMER.split("."))}'
        self.peg_from_time: str = f'FROM.[{"].".join(self.TIME.split("."))}'
        self.peg_to_item: str = self.peg_from_item.replace("FROM.", "TO.")
        self.peg_to_location: str = self.peg_from_location.replace("FROM.", "TO.")
        self.peg_to_customer: str = self.peg_from_customer.replace("FROM.", "TO.")
        self.peg_to_time: str = self.peg_from_time.replace("FROM.", "TO.")
        self.peg_order_index: str = "OrderIndex"
        self.peg_forecast_index: str = "ForecastIndex"
        self.peg_forecast_measure: str = (
            "005.006 Common Demand Netting Pegging.[Pegging Measure Name]"
        )
        self.peg_qty_consumed: str = (
            "005.006 Common Demand Netting Pegging.[Quantity Consumed]"
        )
        self.order_consumed: str = "order_consumed"
        self.order_remaining: str = "order_remaining"
        self.forecast_consumed: str = "forecast_consumed"
        self.forecast_remaining: str = "forecast_remaining"
        self.orderNotCoverByRTF: str = "ORDER_NOT_COVERED_BY_RTF"
        self.orderCoverByRTF: str = "ORDER_COVERED_BY_RTF"
        self.is_base: str = "Is Base Forecast"
        self.NATIVE_CONSUME: str = "NATIVE_CONSUME"
        self.remaining_order_after_forecast: str = "remaining_order_after_forecast"
        self.order_consumed_by_all_forecast: str = "order_consumed_by_all_forecast"

        self.unique_forecast_item: set = set()
        self.unique_forecast_location: set = set()
        self.unique_forecast_customer: set = set()
        self.unique_forecast_time: set = set()
        self.orders_seen: set = set()
        self.forecasts_seen: set = set()
        self.all_time_buckets: list = []
        self.default_demand_ids: list = []
        self.pegging: list = []
        self.original_forecast_grain: list = []
        self.order_grain: list = [self.ITEM, self.LOCATION, self.CUSTOMER, self.TIME]
        self.forecast_grain: list = [
            self.f_item,
            self.f_location,
            self.f_customer,
            self.f_time,
        ]
        time_columns_without_keys: list = [
            column for column in self.master_time.columns if "Key" in column
        ]
        self.forecastToIndexMap: dict = {}
        self.originalForecastToIndexMap: dict = {}
        self.forecastIndexForPeggingMap: dict = {}
        self.item_map: dict = {}
        self.customer_map: dict = {}
        self.time_map: dict = {}
        self.location_map: dict = {}
        self.order_forecast_map_hash: dict = {}
        self.order_consumption_tuples: dict = {}
        self.os_map: dict = {}
        self.fs_map: dict = {}
        self.empty_forecast_indices: dict = {}
        self.orderQtyHash: dict = {}
        self.forecastQtyHash: dict = {}
        self.originalForecastQtyHash: dict = {}
        self.past_order_hash: dict = {}
        self.item_col_hierarchy: dict = {
            index: column
            for index, column in enumerate(reversed(self.master_item.columns))
        }
        self.location_col_hierarchy: dict = {
            index: column
            for index, column in enumerate(reversed(self.master_location.columns))
        }
        self.customer_col_hierarchy: dict = {
            index: column
            for index, column in enumerate(reversed(self.master_customer.columns))
        }
        self.time_col_hierarchy: dict = {
            index: column
            for index, column in enumerate(reversed(time_columns_without_keys))
        }
        self.time_priority_data: DataFrame = DataFrame()
        self.original_in_forecast: DataFrame = DataFrame()
        self.finalPegging: dict = defaultdict(list)

        # Log
        self.plugin_log(f"Class Version: {len(self.class_version)}")
        self.plugin_log(f"Order: {len(self.in_orders)}")
        self.plugin_log(f"Forecast: {len(self.in_forecasts)}")
        self.plugin_log(f"RTF: {len(self.in_RTFs)}")
        self.plugin_log(
            f"Use Order Forecast Graph Mapping: {self.use_order_forecast_map}"
        )
        self.plugin_log(
            f"Prioritize Order and Forecast by Time Bucket: {self.prioritize_order_forecast_by_time}"
        )
        # self.plugin_log(f"RTF Netting Enabled: {self.RTFNettingFlag}")
        self.plugin_log(f"Pegging Enabled: {self.pegging_flag}")
        self.plugin_log(f"Ignore Buckets: {self.no_bucket_flag}")
        self.plugin_log(f"Split Demand Type: {self.split_demand_type}")
        self.plugin_log(f"Using Aggregate Netting: {self.use_aggregate}")
        self.plugin_log(f"Using Multistream Netting: {self.use_multi_stream}")

        if not self.use_order_forecast_map:
            self.in_orderForecastMapGraph = DataFrame()

        self.tuple_size_warning_counter: int = 0
        self.final_time_attribute: str = self.parameters.get(
            Config.DN_OUT_FINAL_TIME_ATTR, Config.OUT_FINAL_TIME_ATTR
        )

        self.final_time_attribute = to_key_col(self.final_time_attribute)
        self.order_id_due_date_map: DataFrame = DataFrame()
        self.profile_output: bool = True

    def plugin_log(self, _msg, _type=""):
        elapsed = time() - self.startTime
        pre = f"{self.class_name}_{self.class_version}"
        if _type == "warn":
            self.logger.warning(f"{pre}: {_msg}: Elapsed Time - {elapsed} seconds")
        elif _type == "error":
            self.logger.error(f"{pre}: {_msg}: Elapsed Time - {elapsed} seconds")
        elif _type == "debug":
            self.logger.debug(f"{pre}: {_msg}: Elapsed Time - {elapsed} seconds")
        else:
            print(f"{pre}: {_msg}: Elapsed Time - {elapsed} seconds")
            self.logger.info(f"{pre}: {_msg}: Elapsed Time - {elapsed} seconds")

    def customTimeDelta(self, timeBucket: str, offset: any):
        """Return the number of days in given day, week or month offset"""
        delta = datetime.timedelta(0)
        try:
            if timeBucket == "Time.[DayKey]":
                delta = datetime.timedelta(days=offset)
            elif timeBucket == "Time.[WeekKey]":
                delta = datetime.timedelta(weeks=offset)
            elif timeBucket == "Time.[MonthKey]":
                delta = DateOffset(months=offset)
            return delta
        except Exception as e:
            self.plugin_log("Cannot calculate timedelta...", _type="warn")
            raise Exception(f"Exception : {e}")

    def split_on_order_horizon(self):
        output_demand_types, output_forecast_types = DataFrame(), DataFrame()
        if self.order_horizon <= 0:
            pass
            # max_order_date = (to_datetime(self.in_orders[self.TIME])).max()
            # max_forward_bucket_size = (self.in_orders[self.FORWARD_BUCKETS]).max()
            # if isna(max_forward_bucket_size):
            #     max_forward_bucket_size = 0
            # horizon_date = max_order_date + self.customTimeDelta(self.TIME, max_forward_bucket_size)
            # self.in_orders = self.in_orders[to_datetime(self.in_orders[self.TIME]) <= horizon_date]
            # self.in_forecasts = self.in_forecasts[to_datetime(self.in_forecasts[self.f_time]) <= horizon_date]
        else:
            current_date = to_datetime(self.in_pastOrderDate[self.TIME][0])

            horizon_date = current_date + self.customTimeDelta(
                self.TIME, self.order_horizon - 1
            )

            skip_orders = self.in_orders[
                to_datetime(self.in_orders[self.TIME]) > horizon_date
                ]
            skip_forecasts = self.in_forecasts[
                to_datetime(self.in_forecasts[self.f_time]) > horizon_date
                ]

            self.in_orders = self.in_orders[
                to_datetime(self.in_orders[self.TIME]) <= horizon_date
                ]
            self.in_forecasts = self.in_forecasts[
                to_datetime(self.in_forecasts[self.f_time]) <= horizon_date
                ]

            skip_netting = SkipNetting(
                in_orders=skip_orders,
                in_forecasts=skip_forecasts,
                in_forecastStreamParameters=self.in_forecastStreamParameters,
                in_parameters=self.in_parameters,
                logger=self.logger,
            )

            output_demand_types, output_forecast_types = skip_netting.run_skip_netting()

        return output_demand_types, output_forecast_types

    def run_demand_netting(self) -> (DataFrame, DataFrame, DataFrame):
        # self.order_horizon = 5
        # Initialize Outputs
        demand_type_grain: list = [
            self.VERSION,
            self.ITEM,
            self.LOCATION,
            self.CUSTOMER,
            self.final_time_attribute,
            self.DEMAND_ID,
            self.DEMAND_TYPE,
            self.netted_demand_qty,
        ]
        pegging_grain = [
            self.VERSION,
            self.peg_from_demand_id,
            self.peg_from_item,
            self.peg_from_location,
            self.peg_from_customer,
            self.peg_from_time,
            self.peg_to_item,
            self.peg_to_location,
            self.peg_to_customer,
            self.peg_to_time,
            self.peg_qty_consumed,
            self.peg_forecast_measure,
        ]
        order_demand_type_output: DataFrame = DataFrame(columns=demand_type_grain)
        forecast_demand_type_output: DataFrame = DataFrame(columns=demand_type_grain)
        pegging_output: DataFrame = DataFrame(columns=pegging_grain)

        try:
            self.early_exit_conditions()
            self.order_id_due_date_map = self.in_orders[
                [self.DEMAND_ID, self.order_due_date]
            ]
            self.order_id_due_date_map.drop_duplicates(inplace=True)
            self.order_id_due_date_map.reset_index(inplace=True, drop=True)

            if self.skip_netting or self.order_horizon == 0:
                # Add skip Netting code
                skip_netting = SkipNetting(
                    in_orders=self.in_orders,
                    in_forecasts=self.in_forecasts,
                    in_forecastStreamParameters=self.in_forecastStreamParameters,
                    in_parameters=self.in_parameters,
                    logger=self.logger,
                )
                order_demand_type_output, forecast_demand_type_output = (
                    skip_netting.run_skip_netting()
                )
            else:
                skip_order_output, skip_forecast_output = self.split_on_order_horizon()
                # Clean Inputs.
                self.preprocess_inputs()
                if self.use_aggregate:
                    # Get Aggregate Grains
                    self.get_aggregate_grains()
                # Clean Order and Forecast Data
                self.setup_orders()
                self.setup_forcast()
                # Create Forecast Lookups.
                self.create_forecast_lookup()
                # Create Hierarchy Maps.
                self.create_hierarchical_maps()
                # Set Order Priority
                self.set_order_priority()
                # Core Netting
                if self.use_order_forecast_map:
                    self.run_graph_netting()
                else:
                    if self.use_multi_stream:
                        self.run_multistream_netting()
                    else:
                        if self.use_aggregate:
                            self.run_aggregate_netting()
                        else:
                            self.run_common_netting()
                # Get Demand Type.
                print()
                order_demand_type_output, forecast_demand_type_output = (
                    self.get_demand_types()
                )
                if self.use_multi_stream:
                    if not order_demand_type_output.empty:
                        self.past_orders = self.past_orders[
                            list(order_demand_type_output.columns)
                        ]
                        output_demand_types = concat(
                            [order_demand_type_output, self.past_orders], ignore_index=True
                        )
                # Telescopic Time
                if self.use_aggregate and self.output_at_aggregated_level:
                    self.ITEM = self.f_item
                    self.LOCATION = self.f_location
                    self.CUSTOMER = self.f_customer
                    self.TIME = self.f_time
                    self.profile_output = False

                # Get Pegging Data.
                pegging_output = self.get_pegging_data()
                # concatenating outputs from skip netting
                order_demand_type_output = concat(
                    [order_demand_type_output, skip_order_output], ignore_index=True
                )
                forecast_demand_type_output = concat(
                    [forecast_demand_type_output, skip_forecast_output], ignore_index=True
                )
                # Decimal Issue
                order_demand_type_output[self.netted_demand_qty] = order_demand_type_output[
                    self.netted_demand_qty
                ].round(4)
                forecast_demand_type_output[self.netted_demand_qty] = (
                    forecast_demand_type_output[self.netted_demand_qty].round(4)
                )

                order_demand_type_output = order_demand_type_output[
                    [
                        self.VERSION,
                        self.ITEM,
                        self.LOCATION,
                        self.CUSTOMER,
                        self.TIME,
                        self.DEMAND_ID,
                        self.DEMAND_TYPE,
                        self.netted_demand_qty,
                    ]
                ]

                forecast_demand_type_output = forecast_demand_type_output[
                    [
                        self.VERSION,
                        self.ITEM,
                        self.LOCATION,
                        self.CUSTOMER,
                        self.TIME,
                        self.DEMAND_ID,
                        self.DEMAND_TYPE,
                        self.netted_demand_qty,
                    ]
                ]

            self.plugin_log("Netting Instance Completed.")

            # Profiling class call
            if self.profile_output:
                order_demand_type_output = order_demand_type_output.merge(
                    self.order_id_due_date_map, on=[self.DEMAND_ID], how="inner"
                )
                prof_obj = Profiling(
                    in_netted_order=order_demand_type_output,
                    in_netted_forecast=forecast_demand_type_output,
                    in_basis=self.in_basis,
                    in_parameters=self.in_parameters,
                    in_telescopic=self.in_telescopic,
                    logger=self.logger,
                )

                order_demand_type_output, forecast_demand_type_output = (
                    prof_obj.run_profiling()
                )

            finalOrderHeaders = [
                self.VERSION,
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.final_time_attribute,
                self.DEMAND_ID,
                self.DEMAND_TYPE,
                self.netted_demand_qty,
            ]

            finalForecastHeaders = [
                self.VERSION,
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.final_time_attribute,
                self.DEMAND_ID,
                self.DEMAND_TYPE,
                self.netted_demand_qty,
            ]

            order_demand_type_output = order_demand_type_output[finalOrderHeaders]
            forecast_demand_type_output = forecast_demand_type_output[finalForecastHeaders]

            order_demand_type_output = order_demand_type_output.groupby(by=finalOrderHeaders[:-1], as_index=False,
                                                                        observed=True).agg(
                {
                    self.netted_demand_qty: "sum"
                }
            )

            forecast_demand_type_output = forecast_demand_type_output.groupby(by=finalOrderHeaders[:-1], as_index=False,
                                                                              observed=True).agg(
                {
                    self.netted_demand_qty: "sum"
                }
            )

            order_demand_type_output[self.final_time_attribute] = to_datetime(
                order_demand_type_output[self.final_time_attribute])
            forecast_demand_type_output[self.final_time_attribute] = to_datetime(
                forecast_demand_type_output[self.final_time_attribute])

            pegging_output[self.peg_from_time] = to_datetime(pegging_output[self.peg_from_time])
            pegging_output[self.peg_to_time] = to_datetime(pegging_output[self.peg_to_time])

            order_demand_type_output = self.col_name_reorder(order_demand_type_output)
            forecast_demand_type_output = self.col_name_reorder(forecast_demand_type_output)
        except PluginException as e:
            self.plugin_log(e)

        return (
            order_demand_type_output,
            forecast_demand_type_output,
            pegging_output,
        )

    def col_name_reorder(self, df):
        dim_cols = [col for col in df.columns if ".[" in col]
        measure_cols = [col for col in df.columns if ".[" not in col]
        final_cols = dim_cols + measure_cols
        df = df[final_cols]
        return df

    def exit_condition(self, condition, _msg):
        if condition:
            self.plugin_log(_msg, "warn")
            raise PluginException(_msg)

    def early_exit_conditions(self):
        # Early Exit Condition
        orderHeader = list(self.in_orders.columns)
        forecastHeader = list(self.in_forecasts.columns)
        rtfHeader = list(self.in_RTFs.columns)

        conditions = [
            (
                self.DEMAND_ID not in orderHeader,
                f"{self.DEMAND_ID} missing from Order input.",
            ),
            (self.ITEM not in orderHeader, f"{self.ITEM} missing from Order Data."),
            (
                self.LOCATION not in orderHeader,
                f"{self.LOCATION} missing from Order Data.",
            ),
            (
                self.CUSTOMER not in orderHeader,
                f"{self.CUSTOMER} missing from Order Data.",
            ),
            (self.TIME not in orderHeader, f"{self.TIME} missing from Order Data."),
            (
                self.ITEM not in forecastHeader,
                f"{self.ITEM} missing from Forecast Data.",
            ),
            (
                self.LOCATION not in forecastHeader,
                f"{self.LOCATION} missing from Forecast Data.",
            ),
            (
                self.CUSTOMER not in forecastHeader,
                f"{self.CUSTOMER} missing from Forecast Data.",
            ),
            (
                self.TIME not in forecastHeader,
                f"{self.TIME} missing from Forecast Data.",
            ),
            (
                self.forecast_qty not in forecastHeader,
                f"Missing forecastQTY measure: {self.forecast_qty}  from Forecast Data.",
            ),
            (
                len(self.in_orders) == 0 and len(self.in_forecasts) == 0,
                "Forecast and Order both are empty. Exiting Netting!",
            ),
            (
                self.UPWARD_ITEM not in orderHeader,
                f"Missing NumberOfUpwardLevelsForItem: {self.UPWARD_ITEM}",
            ),
            (
                self.UPWARD_LOCATION not in orderHeader,
                f"Missing NumberOfUpwardLevelsForLocation: {self.UPWARD_LOCATION}",
            ),
            (
                self.UPWARD_CUSTOMER not in orderHeader,
                f"Missing NumberOfUpwardLevelsForLocation: {self.UPWARD_CUSTOMER}",
            ),
            (
                self.UPWARD_TIME not in orderHeader,
                f"Missing NumberOfUpwardLevelsForTime: {self.UPWARD_TIME}.",
            ),
            (
                self.BACKWARD_BUCKETS not in orderHeader,
                f"Missing BACKWARD_BUCKETS {self.BACKWARD_BUCKETS}.",
            ),
            (
                self.FORWARD_BUCKETS not in orderHeader,
                f"Missing FORWARD_BUCKETS {self.FORWARD_BUCKETS}.",
            ),
            (
                self.EXCLUDE_NETTING not in orderHeader,
                f"Missing ExcludeFromNetting: {self.EXCLUDE_NETTING}.",
            ),
            (
                self.EXCLUDE_PLANNING not in orderHeader,
                f"Missing ExcludeFromPlanning: {self.EXCLUDE_PLANNING}",
            ),
            (
                self.open_order_qty not in orderHeader,
                f"OPEN_ORDER_QTY: {self.open_order_qty} Missing.",
            ),
            (
                self.order_due_date not in orderHeader,
                f"OPEN_ORDER_QTY: {self.order_due_date} Missing.",
            ),
            (
                self.F_BACKWARD_BUCKETS not in forecastHeader,
                f"Missing F_BACKWARD_BUCKETS: {self.F_BACKWARD_BUCKETS}.",
            ),
            (
                self.F_FORWARD_BUCKETS not in forecastHeader,
                f"Missing F_FORWARD_BUCKETS: {self.F_FORWARD_BUCKETS}.",
            ),
            (
                self.F_UPWARD_ITEM not in forecastHeader,
                f"Missing NumberOfUpwardLevelsForItemForecast: {self.F_UPWARD_ITEM}.",
            ),
            (
                self.F_UPWARD_LOCATION not in forecastHeader,
                f"Missing NumberOfUpwardLevelsForLocationForecast: {self.F_UPWARD_LOCATION}.",
            ),
            (
                self.F_UPWARD_CUSTOMER not in forecastHeader,
                f"Missing NumberOfUpwardLevelsForSalesDomainForecast: {self.F_UPWARD_CUSTOMER}.",
            ),
            (
                self.F_UPWARD_TIME not in forecastHeader,
                f"Missing NumberOfUpwardLevelsForTimeForecast: {self.F_UPWARD_TIME}.",
            ),
            (
                self.F_EXCLUDE_NETTING not in forecastHeader,
                f"Missing ExcludeFromNettingForecast: {self.F_EXCLUDE_NETTING}.",
            ),
            (
                self.F_EXCLUDE_PLANNING not in forecastHeader,
                f"Missing ExcludeFromPlanningForecast: {self.F_EXCLUDE_PLANNING}.",
            ),
            (
                self.ITEM not in rtfHeader,
                f"Missing {self.ITEM} from RTF Data.",
            ),
            (
                self.LOCATION not in rtfHeader,
                f"Missing {self.LOCATION} from RTF Data.",
            ),
            (
                self.CUSTOMER not in rtfHeader,
                f"Missing {self.CUSTOMER} from RTF Data.",
            ),
            (
                self.TIME not in rtfHeader,
                f"Missing {self.TIME} from RTF Data.",
            ),
            (
                self.rtf_qty not in rtfHeader,
                f"Missing rtf qty measure: {self.rtf_qty} from RTF Data.",
            ),
        ]
        [self.exit_condition(cond, msg) for cond, msg in conditions]

    def preprocess_inputs(self):
        self.plugin_log(f"Clean Inputs.")
        # Typecast dimension columns of below tables to String
        dimCol = [
            _x for _x in list(self.in_orders.columns) if ".[" in _x
        ]
        self.in_orders[dimCol] = self.in_orders[dimCol].astype(str)
        dimCol = [
            _x
            for _x in list(self.in_forecasts.columns)
            if ".[" in _x
        ]
        self.in_forecasts[dimCol] = self.in_forecasts[dimCol].astype(str)
        self.in_forecasts[self.F_EXCLUDE_NETTING].fillna(False, inplace=True)
        self.in_forecasts[self.F_EXCLUDE_PLANNING].fillna(False, inplace=True)

        dimCol = [_x for _x in list(self.in_RTFs.columns) if ".[" in _x]
        self.in_RTFs[dimCol] = self.in_RTFs[dimCol].astype(str)

        dimCol = [_x for _x in list(self.master_customer.columns) if ".[" in _x]
        self.master_customer[dimCol] = self.master_customer[dimCol].astype(str)

        dimCol = [_x for _x in list(self.master_item.columns) if ".[" in _x]
        self.master_item[dimCol] = self.master_item[dimCol].astype(str)

        dimCol = [_x for _x in list(self.master_location.columns) if ".[" in _x]
        self.master_location[dimCol] = self.master_location[dimCol].astype(str)

        dimCol = [_x for _x in list(self.master_time.columns) if ".[" in _x]
        self.master_time[dimCol] = self.master_time[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_telescopic.columns) if ".[" in _x]
        self.in_telescopic[dimCol] = self.in_telescopic[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_orderStreamParameters.columns) if ".[" in _x]
        self.in_orderStreamParameters[dimCol] = self.in_orderStreamParameters[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_forecastStreamParameters.columns) if ".[" in _x]
        self.in_forecastStreamParameters[dimCol] = self.in_forecastStreamParameters[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_pastOrderDate.columns) if ".[" in _x]
        self.in_pastOrderDate[dimCol] = self.in_pastOrderDate[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_basis.columns) if ".[" in _x]
        self.in_basis[dimCol] = self.in_basis[dimCol].astype(str)

        dimCol = [
            string
            for string in list(self.in_orderForecastMapGraph.columns)
            if "from." in string or "to." in string
        ]
        self.in_orderForecastMapGraph[dimCol] = self.in_orderForecastMapGraph[
            dimCol
        ].astype(str)
        self.in_RTFs = self.in_RTFs[self.in_RTFs[self.rtf_qty].values > 0]
        if len(self.master_item) <= 0:
            self.plugin_log(
                "Item Hierarchy is empty, Hierarchical Netting along Item won't happen.",
                "warn",
            )
        if len(self.master_location) <= 0:
            self.plugin_log(
                "Location data is empty, Hierarchical Netting along Location won't happen.",
                "warn",
            )
        if len(self.master_time) <= 0:
            self.plugin_log(
                "Time data is empty, Hierarchical Netting along Time and Backward/Forecast Netting won't happen.",
                "warn",
            )
        if len(self.master_customer) <= 0:
            self.plugin_log(
                "SalesDomain data is empty, Hierarchical Netting along SalesDomain won't happen.",
                "warn",
            )
        if len(self.in_telescopic) <= 0:
            self.plugin_log(f"TelescopicTime Data is empty.", "warn")

        # GET THE CURRENT VERSION.
        self.curVersion = (
            self.in_orders.iloc[0][self.VERSION]
            if len(self.in_orders) > 0
            else self.in_forecasts.iloc[0][self.VERSION]
        )

        # CREATE BUFFER FORECAST HASH
        if self.use_multi_stream:
            self.default_demand_ids = self.in_forecastStreamParameters[
                self.fs_demand_id
            ].unique()
        else:
            self.default_demand_ids = [Config.FORECAST_DEMAND_ID]

        if len(self.default_demand_ids) == 0:
            self.default_demand_ids = [Config.FORECAST_DEMAND_ID]

        self.original_forecast_grain = self.forecast_grain

    def setup_orders(self):
        self.plugin_log("Cleaning Order Data.")
        # Fill Default
        self.in_orders[self.UPWARD_ITEM].fillna(0, inplace=True)
        self.in_orders[self.UPWARD_LOCATION].fillna(0, inplace=True)
        self.in_orders[self.UPWARD_CUSTOMER].fillna(0, inplace=True)
        self.in_orders[self.UPWARD_TIME].fillna(0, inplace=True)
        self.in_orders[self.BACKWARD_BUCKETS].fillna(0, inplace=True)
        self.in_orders[self.FORWARD_BUCKETS].fillna(0, inplace=True)
        self.in_orders[self.EXCLUDE_NETTING].fillna(False, inplace=True)
        self.in_orders[self.EXCLUDE_PLANNING].fillna(False, inplace=True)
        self.in_orders[
            [
                self.BACKWARD_BUCKETS,
                self.FORWARD_BUCKETS,
                self.UPWARD_ITEM,
                self.UPWARD_LOCATION,
                self.UPWARD_CUSTOMER,
                self.UPWARD_TIME,
            ]
        ] = self.in_orders[
            [
                self.BACKWARD_BUCKETS,
                self.FORWARD_BUCKETS,
                self.UPWARD_ITEM,
                self.UPWARD_LOCATION,
                self.UPWARD_CUSTOMER,
                self.UPWARD_TIME,
            ]
        ].astype(
            int
        )
        # GENERATE TIME PRIORITY
        try:
            self.time_priority_data = self.master_time.copy()
            self.time_priority_data[self.time_key] = to_datetime(
                self.time_priority_data[self.TIME]
            )
            self.time_priority_data.sort_values(by=self.time_key, inplace=True)
            self.time_priority_data[self.time_priority] = arange(len(self.master_time))
        except Exception:
            raise Exception(f"{self.TIME} missing from the Time Hierarchy data.")

        self.in_orders[self.order_qty].fillna(0, inplace=True)
        self.in_orders[self.open_order_qty].fillna(
            self.in_orders[self.order_qty], inplace=True
        )
        self.in_orders = self.in_orders[self.in_orders[self.order_qty].values > 0]

        if self.order_type not in list(self.in_orders.columns):
            self.in_orders[self.order_type] = self.order_qty
        self.in_orders[self.order_type].fillna(self.order_qty, inplace=True)

    def setup_forcast(self):
        self.plugin_log("Cleaning Forecast Data.")
        # CHANGE ORDER GRAIN TO FORECAST GRAIN.
        self.add_forecast_grains_to_order()
        # SORT FORECAST BY ITS GRAIN
        self.in_forecasts.sort_values(by=self.forecast_grain, inplace=True)
        self.in_forecasts.reset_index(inplace=True)
        self.in_forecasts[self.forecast_grain] = self.in_forecasts[
            self.forecast_grain
        ].astype(str)
        # Check this code
        # self.in_forecasts[self.forecast_grain[3]] = to_datetime(
        #     self.in_forecasts[self.forecast_grain[3]]
        # )
        # Fill Default values.
        self.in_forecasts[self.F_BACKWARD_BUCKETS].fillna(0, inplace=True)
        self.in_forecasts[self.F_FORWARD_BUCKETS].fillna(0, inplace=True)
        self.in_forecasts[self.F_UPWARD_ITEM].fillna(0, inplace=True)
        self.in_forecasts[self.F_UPWARD_LOCATION].fillna(0, inplace=True)
        self.in_forecasts[self.F_UPWARD_CUSTOMER].fillna(0, inplace=True)
        self.in_forecasts[self.F_UPWARD_TIME].fillna(0, inplace=True)
        self.in_forecasts[self.F_EXCLUDE_NETTING].fillna(False, inplace=True)
        self.in_forecasts[self.F_EXCLUDE_PLANNING].fillna(False, inplace=True)
        self.in_forecasts[
            [
                self.F_BACKWARD_BUCKETS,
                self.F_FORWARD_BUCKETS,
                self.F_UPWARD_ITEM,
                self.F_UPWARD_LOCATION,
                self.F_UPWARD_CUSTOMER,
                self.F_UPWARD_TIME,
            ]
        ] = self.in_forecasts[
            [
                self.F_BACKWARD_BUCKETS,
                self.F_FORWARD_BUCKETS,
                self.F_UPWARD_ITEM,
                self.F_UPWARD_LOCATION,
                self.F_UPWARD_CUSTOMER,
                self.F_UPWARD_TIME,
            ]
        ].astype(
            int
        )

        self.unique_forecast_item = set(
            list(self.in_forecasts[self.f_item].unique())
            + list(self.in_RTFs[self.f_item].unique())
        )
        self.unique_forecast_location = set(
            list(self.in_forecasts[self.f_location].unique())
            + list(self.in_RTFs[self.f_location].unique())
        )
        self.unique_forecast_customer = set(
            list(self.in_forecasts[self.f_customer].unique())
            + list(self.in_RTFs[self.f_customer].unique())
        )
        self.unique_forecast_time = set(
            list(self.in_forecasts[self.f_time].unique())
            + list(self.in_RTFs[self.f_time].unique())
        )
        self.all_time_buckets = list(self.time_priority_data[self.TIME].unique())

        if self.use_aggregate:
            self.all_time_buckets = list(self.time_priority_data[self.f_time].unique())

    def create_forecast_lookup(self):
        if len(self.in_forecasts) > 0:
            if self.pegging_flag:
                vectorize(self.create_forecast_hash_with_pegging, otypes=[str])(
                    self.in_forecasts[self.f_item],
                    self.in_forecasts[self.f_location],
                    self.in_forecasts[self.f_customer],
                    self.in_forecasts[self.f_time],
                    self.in_forecasts.index,
                    self.forecastToIndexMap,
                    self.forecastIndexForPeggingMap,
                )
            else:
                vectorize(self.create_forecast_hash, otypes=[str])(
                    self.in_forecasts[self.f_item],
                    self.in_forecasts[self.f_location],
                    self.in_forecasts[self.f_customer],
                    self.in_forecasts[self.f_time],
                    self.in_forecasts.index,
                    self.forecastToIndexMap,
                )
            if self.use_aggregate:
                vectorize(self.create_forecast_hash, otypes=[str])(
                    self.original_in_forecast[self.original_forecast_grain[0]],
                    self.original_in_forecast[self.original_forecast_grain[1]],
                    self.original_in_forecast[self.original_forecast_grain[2]],
                    self.original_in_forecast[self.original_forecast_grain[3]],
                    self.original_in_forecast.index,
                    self.originalForecastToIndexMap,
                )

    @staticmethod
    def create_forecast_hash(_item, _loc, _sales, _time, _index, _result):
        if (_item, _loc, _sales) not in _result:
            _result[(_item, _loc, _sales)] = {_time: _index}
        else:
            _result[(_item, _loc, _sales)][_time] = _index

    def create_forecast_hash_with_pegging(
            self, _item, _loc, _sales, _time, _index, _result, _peggingResult
    ):
        self.create_forecast_hash(_item, _loc, _sales, _time, _index, _result)
        _peggingResult[_index] = (_item, _loc, _sales, _time)

    def create_hierarchical_maps(self):
        self.plugin_log("Filter Unused Master Data.")
        # FILTER THE MASTER DATA BASED ON ORDER AND FORECAST
        self.master_location = self.master_location.loc[
            (
                self.master_location[self.LOCATION].isin(
                    self.in_orders[self.LOCATION].unique()
                )
            )
            | (
                self.master_location[self.f_location].isin(
                    self.in_forecasts[self.f_location].unique()
                )
            )
            ]
        self.master_customer = self.master_customer.loc[
            (
                self.master_customer[self.CUSTOMER].isin(
                    self.in_orders[self.CUSTOMER].unique()
                )
            )
            | (
                self.master_customer[self.f_customer].isin(
                    self.in_forecasts[self.f_customer].unique()
                )
            )
            ]
        self.master_item = self.master_item.loc[
            (self.master_item[self.ITEM].isin(self.in_orders[self.ITEM].unique()))
            | (
                self.master_item[self.f_item].isin(
                    self.in_forecasts[self.f_item].unique()
                )
            )
            ]

        self.plugin_log("Creating Hierarchical Maps.")
        # CREATE HIERARCHICAL MAPS
        self.master_item.apply(
            lambda _x: self.hierarchy_data_tree(
                _x.to_dict(), self.item_col_hierarchy, self.item_map
            ),
            axis=1,
        )
        self.master_customer.apply(
            lambda _x: self.hierarchy_data_tree(
                _x.to_dict(), self.customer_col_hierarchy, self.customer_map
            ),
            axis=1,
        )
        self.master_time.apply(
            lambda _x: self.hierarchy_data_tree(
                _x.to_dict(), self.time_col_hierarchy, self.time_map
            ),
            axis=1,
        )
        self.master_location.apply(
            lambda _x: self.hierarchy_data_tree(
                _x.to_dict(), self.location_col_hierarchy, self.location_map
            ),
            axis=1,
        )
        del self.master_location
        del self.master_customer
        del self.master_item

    @staticmethod
    def hierarchy_data_tree(_data, _colHierarchy, _result, _header=0):
        _result[_data[_colHierarchy[_header]]] = {
            key: _data[value] for key, value in _colHierarchy.items() if key != 0
        }
        for key, value in _colHierarchy.items():
            if key != 0:
                if key in _result:
                    if _data[value] in _result[key]:
                        _result[key][_data[value]].append(_data[_colHierarchy[_header]])
                    else:
                        _result[key][_data[value]] = [_data[_colHierarchy[_header]]]
                else:
                    _result[key] = {_data[value]: [_data[_colHierarchy[_header]]]}

    def run_graph_netting(self):
        print(">> Graph Netting")
        # Create Order Consumption Tuples.
        self.create_consumption_tuples_from_graph()
        # Net Order Against Forecast.
        self.net_order_against_forecast()
        # Combine Order with Remaining Forecast.
        self.combine_order_with_forecast()
        # Net Combined Order against RTF.
        self.net_order_against_rtf()

    def run_aggregate_netting(self):
        print(">> Aggregate Netting")
        # Create Order Consumption Tuples.
        self.create_order_consumption_tuples()
        # Net Order Against Forecast.
        self.net_order_against_agg_forecast()
        # Create Forecast Consumption Tuples.
        self.create_forecast_consumption_tuples()
        # Combine Order with Remaining Forecast.
        self.combine_order_with_forecast()
        # Net Combined Order against RTF.
        self.net_order_against_rtf()

    def run_common_netting(self):
        print(">> Common Netting")
        # Create Order Consumption Tuples.
        self.create_order_consumption_tuples()
        # Net Order Against Forecast.
        self.net_order_against_forecast()
        # Create Forecast Consumption Tuples.
        self.create_forecast_consumption_tuples()
        # Combine Order with Remaining Forecast.
        self.combine_order_with_forecast()
        # Net Combined Order against RTF.
        self.net_order_against_rtf()

    def run_multistream_netting(self):
        print(">> MultiStream Netting")
        # Create Order Consumption Tuples.
        self.create_order_consumption_tuples()
        self.separate_past_orders()
        # Net Order Against Forecast.
        self.net_order_against_forecast()
        # Create Forecast Consumption Tuples.
        self.create_forecast_consumption_tuples()
        self.divide_past_orders()
        # Combine Order with Remaining Forecast.
        self.combine_order_with_forecast()
        # Net Combined Order against RTF.
        self.net_order_against_rtf()

    def divide_past_orders(self):
        self.past_orders[self.DEMAND_TYPE] = None
        self.past_orders[self.netted_demand_qty] = self.past_orders[self.order_qty]
        if not self.past_orders.empty:
            for key, value in self.past_order_hash.items():
                self.past_orders.loc[
                    self.past_orders[self.order_type] == key, self.DEMAND_TYPE
                ] = value

        self.past_orders = self.past_orders.dropna(subset=[self.DEMAND_TYPE])

    def get_demand_types(self):

        output_forecast_types: DataFrame = DataFrame()
        output_demand_types: DataFrame = DataFrame()

        mergeGrain = []
        filterGrain = [
            self.ITEM,
            self.LOCATION,
            self.CUSTOMER,
            self.TIME,
        ]

        if self.use_aggregate:
            if self.ITEM != self.f_item:
                mergeGrain.append(self.f_item)
            else:
                mergeGrain.append(self.ITEM)
            if self.LOCATION != self.f_location:
                mergeGrain.append(self.f_location)
            else:
                mergeGrain.append(self.LOCATION)
            if self.CUSTOMER != self.f_customer:
                mergeGrain.append(self.f_customer)
            else:
                mergeGrain.append(self.CUSTOMER)
            if self.TIME != self.f_time:
                mergeGrain.append(self.f_time)
            else:
                mergeGrain.append(self.TIME)

            if not self.output_at_aggregated_level:
                self.f_item = self.ITEM
                self.f_location = self.LOCATION
                self.f_customer = self.CUSTOMER
                self.f_time = self.TIME

        out_grains = [x for x in self.in_orders.columns if ".[" in x]
        if self.split_demand_type:
            for os, os_details in self.os_map.items():
                os_order = self.in_orders[self.in_orders[self.order_type] == os]
                os_order["COM"] = os_order[self.orderCoverByRTF]
                os_order["UNF"] = os_order[
                    [self.orderNotCoverByRTF, self.remaining_order_after_forecast]
                ].min(axis=1)
                os_order["NEW"] = (
                        os_order[self.open_order_qty] - os_order["COM"] - os_order["UNF"]
                )
                for index, os_detail in enumerate(os_details):
                    self.forecast_qty = os_detail[self.os_forecast]
                    fs_detail = self.fs_map[self.forecast_qty]
                    self.forecast_consumed = fs_detail[self.fs_forecast_consumed]
                    unf = os_detail[self.os_unf_order]
                    com = os_detail[self.os_com_order]
                    new = os_detail[self.os_new_order]
                    last = False
                    if index == len(os_details) - 1:
                        last = True
                    out_piece = self.divide_demand(
                        os_order, out_grains, unf, com, new, last
                    )
                    output_demand_types = concat(
                        [output_demand_types, out_piece], ignore_index=True
                    )
        else:
            out_grains = out_grains + [self.DEMAND_TYPE, self.netted_demand_qty]

            os_order = self.in_orders[
                ~(self.in_orders[self.DEMAND_ID].isin(self.default_demand_ids))
            ]

            if not os_order.empty:
                os_order.loc[os_order[self.orderCoverByRTF] > 0, self.DEMAND_TYPE] = (
                    Config.COM_ORDER
                )
                os_order.loc[
                    (
                            (os_order[self.orderCoverByRTF] == 0)
                            & (os_order[self.order_consumed_by_all_forecast] > 0)
                    ),
                    self.DEMAND_TYPE,
                ] = Config.NEW_ORDER
                os_order.loc[
                    (
                            (os_order[self.orderCoverByRTF] == 0)
                            & (os_order[self.order_consumed_by_all_forecast] == 0)
                    ),
                    self.DEMAND_TYPE,
                ] = Config.UNF_ORDER

                os_order[self.netted_demand_qty] = os_order[self.open_order_qty]
                os_order = os_order[out_grains]
            else:
                os_order = DataFrame(columns=out_grains)

            output_demand_types = concat(
                [output_demand_types, os_order], ignore_index=True
            )

        # Dividing the forecast

        for fs, fs_detail in self.fs_map.items():
            forecast_id = fs_detail[self.fs_demand_id]

            fs_order = self.in_orders[self.in_orders[self.DEMAND_ID] == forecast_id]

            fs_order[self.DEMAND_TYPE] = None
            fs_order[self.netted_demand_qty] = 0

            if self.use_aggregate and not self.output_at_aggregated_level:
                self.forecast_remaining = fs_detail[self.fs_forecast_remaining]
                fs_order = fs_order.merge(
                    self.original_in_forecast[
                        list(set(filterGrain + mergeGrain)) + [self.forecast_remaining]
                        ],
                    on=mergeGrain,
                    how="right",
                    suffixes=("_x", ""),
                )

                sumRemForecastHeader = f"SUM_{self.forecast_remaining}"

                fs_order[sumRemForecastHeader] = fs_order.groupby(
                    by=mergeGrain, observed=True
                )[self.forecast_remaining].transform("sum")

                fs_order["Ratio"] = where(
                    fs_order[sumRemForecastHeader] != 0,
                    fs_order[self.forecast_remaining] / fs_order[sumRemForecastHeader],
                    0,
                )

                fs_order[self.orderNotCoverByRTF] *= fs_order["Ratio"]
                fs_order[self.orderCoverByRTF] *= fs_order["Ratio"]

            if self.split_demand_type:
                com = fs_detail[self.fs_com_forecast]
                new = fs_detail[self.fs_new_forecast]
            else:
                com = Config.COM_FCST
                new = Config.NEW_FCST

            com_out = fs_order[out_grains]
            new_out = fs_order[out_grains]

            com_out[self.DEMAND_TYPE] = com
            new_out[self.DEMAND_TYPE] = new

            com_out[self.netted_demand_qty] = fs_order[self.orderCoverByRTF]
            new_out[self.netted_demand_qty] = fs_order[self.orderNotCoverByRTF]

            com_out = com_out[com_out[self.netted_demand_qty] > 0]
            new_out = new_out[new_out[self.netted_demand_qty] > 0]

            output_forecast_types = concat(
                [output_forecast_types, com_out, new_out], ignore_index=True
            )

        return output_demand_types, output_forecast_types

    def divide_demand(
            self,
            os_order,
            out_grains,
            unf,
            com,
            new,
            is_last=False,
    ):
        output_demand_types: DataFrame = DataFrame()
        com_out = os_order[out_grains]
        new_out = os_order[out_grains]
        com_out[self.DEMAND_TYPE] = com
        new_out[self.DEMAND_TYPE] = new
        if is_last:
            com_out[self.netted_demand_qty] = os_order["COM"]
            new_out[self.netted_demand_qty] = os_order["NEW"]

            unf_out = os_order[out_grains]
            unf_out[self.DEMAND_TYPE] = unf
            unf_out[self.netted_demand_qty] = os_order["UNF"]
            unf_out = unf_out[unf_out[self.netted_demand_qty] > 0]

            output_demand_types = concat(
                [output_demand_types, unf_out], ignore_index=True
            )
        else:
            com_out[self.netted_demand_qty] = os_order[
                [self.forecast_consumed, "COM"]
            ].min(axis=1)
            os_order[self.forecast_consumed] -= com_out[self.netted_demand_qty]
            os_order["COM"] -= com_out[self.netted_demand_qty]

            new_out[self.netted_demand_qty] = os_order[
                [self.forecast_consumed, "NEW"]
            ].min(axis=1)
            os_order[self.forecast_consumed] -= new_out[self.netted_demand_qty]
            os_order["NEW"] -= new_out[self.netted_demand_qty]
        com_out = com_out[com_out[self.netted_demand_qty] > 0]
        new_out = new_out[new_out[self.netted_demand_qty] > 0]
        output_demand_types = concat(
            [output_demand_types, com_out, new_out], ignore_index=True
        )

        return output_demand_types

    def create_consumption_tuples_from_graph(self):
        self.plugin_log(
            "Creating Consumptions Tuple for Order Forecast Association Graph."
        )
        item_dim = self.ITEM.split(".[")[0]
        loc_dim = self.LOCATION.split(".[")[0]
        cust_dim = self.CUSTOMER.split(".[")[0]
        for colHeader in self.in_orderForecastMapGraph.columns:
            colHeaderLower = colHeader.lower()
            if "from." in colHeaderLower:
                if item_dim in colHeader:
                    self.from_item = colHeader
                elif loc_dim in colHeader:
                    self.from_location = colHeader
                elif cust_dim in colHeader:
                    self.from_customer = colHeader
            elif "to." in colHeaderLower:
                if item_dim in colHeader:
                    self.to_item = colHeader
                elif loc_dim in colHeader:
                    self.to_location = colHeader
                elif cust_dim in colHeader:
                    self.to_customer = colHeader
        tempGraph = self.in_orderForecastMapGraph[
            self.in_orderForecastMapGraph[self.graph_forecast_association].values == 1
            ]
        tempGraph.sort_values(
            by=[
                self.from_item,
                self.from_location,
                self.from_customer,
                self.graph_priority,
            ],
            inplace=True,
        )

        self.order_forecast_map_hash: dict = {}
        vectorize(self.create_order_forecast_map_hash, otypes=[str])(
            tempGraph[self.from_item],
            tempGraph[self.from_location],
            tempGraph[self.from_customer],
            tempGraph[self.to_item],
            tempGraph[self.to_location],
            tempGraph[self.to_customer],
            self.order_forecast_map_hash,
        )

    @staticmethod
    def create_order_forecast_map_hash(
            _fromItem, _fromLoc, _fromSales, _toItem, _toLoc, _toSales, _result
    ):
        if (_fromItem, _fromLoc, _fromSales) in _result:
            _result[(_fromItem, _fromLoc, _fromSales)].append(
                (_toItem, _toLoc, _toSales)
            )
        else:
            _result[(_fromItem, _fromLoc, _fromSales)] = [(_toItem, _toLoc, _toSales)]
        return ""

    def create_order_consumption_tuples(self):
        self.order_consumption_tuples = {}
        self.plugin_log("Creating Consumptions Tuple.")
        self.plugin_log("Creating Order Tuples.")
        # print(self.Order)
        self.in_orders.apply(
            lambda _x: self.create_tuples_for_order(_x.to_dict()), axis=1
        )

    def create_forecast_consumption_tuples(self):
        self.plugin_log("Creating Consumptions Tuple.")
        self.plugin_log("Creating Forecast Tuples.")
        self.in_forecasts.apply(
            lambda _x: self.create_tuples_for_forecast(_x.to_dict()), axis=1
        )

    def create_tuples_for_order(self, _data):
        oItem = _data[self.ITEM]
        oLocation = _data[self.LOCATION]
        oTime = _data[self.TIME]
        oSales = _data[self.CUSTOMER]
        backward = int(_data[self.BACKWARD_BUCKETS])
        forward = int(_data[self.FORWARD_BUCKETS])
        if (
                oItem,
                oLocation,
                oSales,
                oTime,
                backward,
                forward,
        ) not in self.order_consumption_tuples:
            if self.no_bucket_flag:
                self.order_consumption_tuples[
                    (oItem, oLocation, oSales, oTime, backward, forward)
                ] = [
                    (
                        _data[self.f_item],
                        _data[self.f_location],
                        _data[self.f_customer],
                        _data[self.f_time],
                    )
                ]
            else:
                self.order_consumption_tuples[
                    (oItem, oLocation, oSales, oTime, backward, forward)
                ] = self.create_tuples(
                    _orderData=_data,
                    _backward=backward,
                    _forward=forward,
                    _time_level=int(_data[self.UPWARD_TIME]),
                    _sales_level=int(_data[self.UPWARD_CUSTOMER]),
                    _item_level=int(_data[self.UPWARD_ITEM]),
                    _loc_level=int(_data[self.UPWARD_LOCATION]),
                )

    def create_tuples_for_forecast(self, _data):
        fItem = _data[self.f_item]
        fLocation = _data[self.f_location]
        fSales = _data[self.f_customer]
        fTime = _data[self.f_time]
        backward = int(_data[self.F_BACKWARD_BUCKETS])
        forward = int(_data[self.F_FORWARD_BUCKETS])

        if (
                fItem,
                fLocation,
                fSales,
                fTime,
                backward,
                forward,
        ) not in self.order_consumption_tuples:
            if self.no_bucket_flag:
                self.order_consumption_tuples[
                    (fItem, fLocation, fSales, fTime, backward, forward)
                ] = [
                    (
                        _data[self.f_item],
                        _data[self.f_location],
                        _data[self.f_customer],
                        _data[self.f_time],
                    )
                ]
            else:
                self.order_consumption_tuples[
                    (fItem, fLocation, fSales, fTime, backward, forward)
                ] = self.create_tuples(
                    _orderData=_data,
                    _backward=backward,
                    _forward=forward,
                    _time_level=int(_data[self.F_UPWARD_TIME]),
                    _sales_level=int(_data[self.F_UPWARD_CUSTOMER]),
                    _item_level=int(_data[self.F_UPWARD_ITEM]),
                    _loc_level=int(_data[self.F_UPWARD_LOCATION]),
                )

    def create_tuples(
            self,
            _orderData,
            _backward,
            _forward,
            _time_level,
            _sales_level,
            _item_level,
            _loc_level,
    ) -> list:
        """
        createTuples(_orderData, _dimension, _level, _backward, _forward):
        1. Calculate current and BACKWARD AND FORWARD time buckets.
        curTimeBucket = 'W3'
        backward_buckets_values = ['W2', 'W1']
        forward_buckets_values = ['W4', 'W5']

        2. Get all upwards header data (LOCATION) from UPWARD LEVEL 1 to _level
        upwardHeaderData = [_orderData[forecastHeader]]
        upwardHeaderData = ['ShipTo_3', 'ShipTo_4', 'CDC_10', 'Supplier_4']

        3. COMBINE TIME DATA AND UPWARD DATA TO CREATE THE TUPLES.
        :param _orderData:
        :param _time_level:
        :param _item_level:
        :param _loc_level:
        :param _sales_level:
        :param _backward:
        :param _forward:
        :return:
        """

        fItem = _orderData[self.f_item]
        fLoc = _orderData[self.f_location]
        fSales = _orderData[self.f_customer]
        fTime = _orderData[self.f_time]
        backward_buckets_values = []
        forward_buckets_values = []
        upward_item_values = [fItem]
        upward_location_values = [fLoc]
        upward_customer_values = [fSales]
        upward_time_values = [fTime]

        if _backward > 0:
            backward_buckets_values = self.get_backward_time(fTime, _backward)
        if _forward > 0:
            forward_buckets_values = self.get_forward_time(fTime, _forward)
        if self.enable_time_hierarchy:
            upward_time_values = self.get_siblings(
                self.time_map,
                _time_level,
                self.unique_forecast_time,
                _orderData[self.TIME],
            )
        if _item_level > 0:
            upward_item_values = self.get_siblings(
                self.item_map,
                _item_level,
                self.unique_forecast_item,
                _orderData[self.ITEM],
            )
        if _loc_level > 0:
            upward_location_values = self.get_siblings(
                self.location_map,
                _loc_level,
                self.unique_forecast_location,
                _orderData[self.LOCATION],
            )
        if _sales_level > 0:
            upward_customer_values = self.get_siblings(
                self.customer_map,
                _sales_level,
                self.unique_forecast_customer,
                _orderData[self.CUSTOMER],
            )
        hierarchicalConsumptionOrder = self.parameters[Config.DN_H_CONSUMPTION_ORDER]
        consumptionOrder = self.parameters[Config.DN_CONSUMPTION_ORDER]
        if self.enable_time_hierarchy:
            chars = "ILST"
            validHierarchyConsOrder = set(
                ["".join(perm) for perm in permutations(chars, 4)]
            )
            if hierarchicalConsumptionOrder not in validHierarchyConsOrder:
                raise PluginException(
                    f"Incorrect {Config.DN_H_CONSUMPTION_ORDER}: {hierarchicalConsumptionOrder}. Exiting!"
                )
            data_map = {
                "I": upward_item_values,
                "L": upward_location_values,
                "S": upward_customer_values,
                "T": upward_time_values,
            }
            consumption_tuple = self.form_consumption_tuples(
                data_map, hierarchicalConsumptionOrder, "T"
            )

        else:
            chars = "BILSF"
            validConsOrder = set(["".join(perm) for perm in permutations(chars, 5)])
            # Making sure Backward is done before Forward
            validConsOrder = {
                perm for perm in validConsOrder if perm.index("B") < perm.index("F")
            }
            if consumptionOrder not in validConsOrder:
                raise PluginException(
                    f"Incorrect {Config.DN_CONSUMPTION_ORDER}: {consumptionOrder}. Exiting!"
                )
            backwardConsOrder = consumptionOrder.replace("F", "")
            forwardConsOrder = consumptionOrder.replace("B", "")
            if self.enable_backward_before_current:
                backward_buckets_values = backward_buckets_values + [
                    upward_time_values[0]
                ]
            else:
                backward_buckets_values = [
                                              upward_time_values[0]
                                          ] + backward_buckets_values
            backward_data_map = {
                "I": upward_item_values,
                "L": upward_location_values,
                "S": upward_customer_values,
                "B": backward_buckets_values,
            }
            # print(">>>", backwardConsOrder)
            consumption_tuple = self.form_consumption_tuples(
                backward_data_map, backwardConsOrder, "B"
            )
            forward_data_map = {
                "I": upward_item_values,
                "L": upward_location_values,
                "S": upward_customer_values,
                "F": forward_buckets_values,
            }
            consumption_tuple += self.form_consumption_tuples(
                forward_data_map, forwardConsOrder, "F"
            )

        if len(consumption_tuple) >= 10000 and self.tuple_size_warning_counter < 10:
            self.plugin_log(
                f"Consumption Tuple size is greater than 10k for order: {_orderData[self.DEMAND_ID]}",
                _type="warn",
            )
            self.tuple_size_warning_counter += 1
            if self.tuple_size_warning_counter == 10:
                self.plugin_log(
                    "There can be orders for which tuple size is large, Netting might run slower",
                    _type="warn",
                )

        return consumption_tuple

    def get_backward_time(self, _fTime, _backward) -> list:
        result = []
        curTimeIndex = self.all_time_buckets.index(_fTime)
        for backwardBucket in range(1, int(_backward) + 1):
            backwardIndex = curTimeIndex - backwardBucket
            if backwardIndex < 0:
                continue
            timeBucket = self.all_time_buckets[backwardIndex]
            result.append(timeBucket)
        if self.enable_backward_before_current:
            result = result[::-1]
        return result

    def get_forward_time(self, _fTime, _forward) -> list:
        result = []
        curTimeIndex = self.all_time_buckets.index(_fTime)
        for forwardBucket in range(1, int(_forward) + 1):
            forwardIndex = curTimeIndex + forwardBucket
            if forwardIndex >= len(self.all_time_buckets):
                continue
            timeBucket = self.all_time_buckets[forwardIndex]
            result.append(timeBucket)
        return result

    @staticmethod
    def get_siblings(_map, _level, _uniqueValues, _orderValue) -> list:
        result = [_orderValue]
        seen_values = set(result)

        for i in range(1, _level + 1):
            hierarchyCol = _map[_orderValue].get(i, None)
            if hierarchyCol is None:
                break

            siblings = [
                value
                for value in _map[i].get(hierarchyCol, [])
                if value in _uniqueValues and value not in seen_values
            ]
            result.extend(siblings)
            seen_values.update(siblings)

        return result

    @staticmethod
    def form_consumption_tuples(
            data_map,
            consumption_order,
            time_tuple_value="T",
    ) -> list:
        result = []
        seen_tuple = set()
        tuple_dict = {"I": None, "L": None, "S": None, "B": None, "T": None, "F": None}
        for fourth in data_map[consumption_order[3]]:
            for third in data_map[consumption_order[2]]:
                for second in data_map[consumption_order[1]]:
                    for first in data_map[consumption_order[0]]:
                        tuple_dict[consumption_order[0]] = first
                        tuple_dict[consumption_order[1]] = second
                        tuple_dict[consumption_order[2]] = third
                        tuple_dict[consumption_order[3]] = fourth
                        reordered_vals = (
                            tuple_dict["I"],
                            tuple_dict["L"],
                            tuple_dict["S"],
                            tuple_dict[time_tuple_value],
                        )
                        # print(reordered_vals)
                        if reordered_vals not in seen_tuple:
                            seen_tuple.add(reordered_vals)
                            result.append(reordered_vals)
        return result

    def set_order_priority(self, _isForecast=False):
        orderPriority = []
        orderHeaders = self.in_orders.columns
        order_time_priority = "order_time_priority"
        if self.order_priority in orderHeaders:
            if len(self.in_orders[self.order_priority].unique()) > 2:
                orderPriority.append(self.order_priority)
                self.in_orders[self.order_priority] = self.in_orders[
                    self.order_priority
                ].fillna(self.in_orders[self.order_priority].max() + 1)
        if order_time_priority in list(orderHeaders):
            self.in_orders.drop(columns=order_time_priority, inplace=True)
        timeOrderHeader = self.f_time if _isForecast else self.TIME
        curTimePriorityData = (
            self.time_priority_data[[timeOrderHeader, self.time_priority]]
            .groupby(timeOrderHeader, as_index=False)
            .agg("max")
        )

        self.in_orders = merge(
            self.in_orders, curTimePriorityData, on=timeOrderHeader, how="left"
        )
        self.in_orders.rename(
            columns={self.time_priority: order_time_priority}, inplace=True
        )
        # CALCULATE ORDER PRIORITY BASED ON MEASURE GIVEN AND TIE-BREAKER (Demand ID).
        orderPriority.append(order_time_priority)
        orderPriority.append(self.DEMAND_ID)
        # SORT ORDERS
        if _isForecast:
            if self.prioritize_order_forecast_by_time_flag:
                maxPriorityOrder = 0.1
            else:
                tmp = self.in_orders.loc[
                    self.in_orders[self.DEMAND_ID].isin(self.default_demand_ids),
                    order_time_priority,
                ]
                if len(tmp) > 0:
                    maxPriorityOrder = int(tmp.max(skipna=True)) + 1
                else:
                    maxPriorityOrder = 999999
            self.in_orders.loc[
                self.in_orders[self.DEMAND_ID].isin(self.default_demand_ids),
                order_time_priority,
            ] += maxPriorityOrder
            orderPriority = orderPriority + self.forecast_grain[:2]

        self.plugin_log(f"Sorting ({self.order_qty}) via: {orderPriority}")
        self.in_orders.sort_values(by=orderPriority, inplace=True)

    def net_order_against_forecast(self):
        self.generate_stream_hash()

        if self.use_multi_stream:
            for forecast_stream in list(self.fs_map.keys()):
                self.in_orders[
                    self.fs_map[forecast_stream][self.fs_forecast_consumed]
                ] = 0
                self.in_forecasts[
                    self.fs_map[forecast_stream][self.fs_forecast_consumed]
                ] = 0
                if forecast_stream in list(self.in_forecasts.columns):
                    self.in_forecasts[
                        self.fs_map[forecast_stream][self.fs_forecast_remaining]
                    ] = self.in_forecasts[forecast_stream]
                else:
                    _msg = f"{forecast_stream} missing in forecast Data"
                    self.plugin_log(_msg, "warn")
                    raise PluginException(_msg)

        self.in_orders[self.remaining_order_after_forecast] = self.in_orders[self.order_qty]

        for os, os_details in self.os_map.items():
            for os_detail in os_details:
                if os not in self.past_order_hash:
                    self.past_order_hash[os] = os_detail[self.os_past_order_dt]
                self.order_consumed = os_detail[self.os_order_consumed]
                self.order_remaining = os_detail[self.os_order_remaining]
                self.forecast_qty = os_detail[self.os_forecast]
                fs_detail = self.fs_map[self.forecast_qty]
                self.forecast_consumed = fs_detail[self.fs_forecast_consumed]
                self.forecast_remaining = fs_detail[self.fs_forecast_remaining]
                self.in_forecasts[self.forecast_qty].fillna(0, inplace=True)
                self.empty_forecast_indices = {}
                self.pegging = []
                self.run_netting(os, _excludeOrderMeasure=self.EXCLUDE_NETTING)
                self.orders_seen.add(self.order_consumed)
                self.orders_seen.add(self.order_remaining)
                self.forecasts_seen.add(self.forecast_consumed)

                if self.pegging_flag:
                    self.append_to_final_pegging(self.pegging)

        self.in_orders[self.order_consumed_by_all_forecast] = (
                self.in_orders[self.order_qty]
                - self.in_orders[self.remaining_order_after_forecast]
        )

        self.in_orders[self.remaining_order_after_forecast] = (
                self.in_orders[self.open_order_qty]
                - self.in_orders[self.order_consumed_by_all_forecast]
        )

        self.in_orders.loc[
            self.in_orders[self.remaining_order_after_forecast] < 0,
            self.remaining_order_after_forecast,
        ] = 0

    def populate_order_stream_hash(self, order_stream_param):
        order_stream = order_stream_param[self.os_stream]
        forecast_stream = order_stream_param[self.os_forecast]
        if isna(order_stream) or isna(forecast_stream):
            return
        order_measure_without_space = order_stream.replace(" ", "")
        consumedOrder = f"consumed_{order_measure_without_space}"
        remainingOrder = f"remaining_{order_measure_without_space}"

        is_rtf = order_stream_param.get(self.os_is_rtf, True)
        com_order = order_stream_param.get(self.os_com_order, Config.COM_ORDER)
        new_order = order_stream_param.get(self.os_new_order, Config.NEW_ORDER)
        unf_order = order_stream_param.get(self.os_unf_order, Config.UNF_ORDER)
        past_order = order_stream_param.get(self.os_past_order_dt, Config.UNF_ORDER)

        if order_stream in self.os_map:
            self.os_map[order_stream].append(
                {
                    self.os_forecast: forecast_stream,
                    self.os_is_rtf: is_rtf,
                    self.os_com_order: com_order,
                    self.os_new_order: new_order,
                    self.os_unf_order: unf_order,
                    self.os_past_order_dt: past_order,
                    self.os_order_consumed: consumedOrder,
                    self.os_order_remaining: remainingOrder,
                }
            )
        else:
            self.os_map[order_stream] = [
                {
                    self.os_forecast: forecast_stream,
                    self.os_is_rtf: is_rtf,
                    self.os_com_order: com_order,
                    self.os_new_order: new_order,
                    self.os_unf_order: unf_order,
                    self.os_past_order_dt: past_order,
                    self.os_order_consumed: consumedOrder,
                    self.os_order_remaining: remainingOrder,
                }
            ]

    def populate_forecast_stream_hash(self, forecast_stream_param):
        forecast_stream = forecast_stream_param[self.fs_stream]
        if isna(forecast_stream):
            return
        forecast_stream_without_space = forecast_stream.replace(" ", "")
        consumedForecast = f"consumed_{forecast_stream_without_space}"
        remainingForecast = f"remaining_{forecast_stream_without_space}"
        self.fs_map[forecast_stream] = {
            self.fs_demand_id: forecast_stream_param[self.fs_demand_id],
            self.fs_is_rtf: forecast_stream_param[self.fs_is_rtf],
            self.fs_com_forecast: forecast_stream_param[self.fs_com_forecast],
            self.fs_new_forecast: forecast_stream_param[self.fs_new_forecast],
            self.fs_forecast_consumed: consumedForecast,
            self.fs_forecast_remaining: remainingForecast,
            self.is_base: (
                True if "base" in forecast_stream_without_space.lower() else False
            ),
        }

    def run_netting(self, _os=None, _excludeOrderMeasure=None):
        """
        mainFunction: Starting Function.
        Add OrderQuantityNotCoveredByForecast and OrderQuantityCoveredByForecast to order data.
        Add NettedForecastQuantityOutput and ConsumedForecastQuantityOutput to forecast data.
        Iterate over order data to call processOrder Function
        """
        # print(">> run net")
        if self.order_remaining not in self.in_orders.columns:
            self.orderQtyHash = self.in_orders[self.remaining_order_after_forecast].to_dict()
            self.in_orders[self.order_remaining] = self.in_orders[self.remaining_order_after_forecast]
            self.in_orders[self.order_consumed] = 0
        else:
            self.orderQtyHash = self.in_orders[self.order_remaining].to_dict()
        if self.forecast_remaining not in self.in_forecasts.columns:
            self.forecastQtyHash = self.in_forecasts[self.forecast_qty].to_dict()
        else:
            self.forecastQtyHash = self.in_forecasts[self.forecast_remaining].to_dict()

        # PROCESS ORDER.
        if _os is not None:
            self.plugin_log(
                f"Run Netting For ({self.order_qty}"
                f": {len(self.in_orders[(self.in_orders[self.order_qty].values > 0) & (self.in_orders[self.order_type] == _os)])} "
                f":: {self.forecast_qty}: {len(self.in_forecasts[self.in_forecasts[self.forecast_qty].values > 0])})"
            )
            self.in_orders[
                (self.in_orders[self.order_qty].values > 0)
                & (self.in_orders[self.order_type] == _os)
                ].apply(
                lambda _x: self.process_order(
                    _x.to_dict(), _x.name, _excludeOrderMeasure
                ),
                axis=1,
            )
        else:
            self.plugin_log(
                f"Run Netting For ({self.order_qty}"
                f": {len(self.in_orders[(self.in_orders[self.order_qty].values > 0)])} "
                f":: {self.forecast_qty}: {len(self.in_forecasts[self.in_forecasts[self.forecast_qty].values > 0])})"
            )
            self.in_orders[(self.in_orders[self.order_qty].values > 0)].apply(
                lambda _x: self.process_order(
                    _x.to_dict(), _x.name, _excludeOrderMeasure
                ),
                axis=1,
            )

        if self.use_multi_stream:
            self.in_orders[self.forecast_consumed] = self.in_orders[self.forecast_consumed] + (
                    self.in_orders[self.order_remaining]
                    - Series(self.orderQtyHash)
            )
        self.in_orders[self.remaining_order_after_forecast] = Series(self.orderQtyHash)
        self.in_orders[self.order_consumed] = (
                self.in_orders[self.order_qty] - self.in_orders[self.remaining_order_after_forecast])
        self.in_orders[self.order_remaining] = Series(self.orderQtyHash)
        self.in_forecasts[self.forecast_remaining] = Series(self.forecastQtyHash)
        self.in_forecasts[self.forecast_consumed] = (
                self.in_forecasts[self.forecast_qty].values
                - self.in_forecasts[self.forecast_remaining].values
        )

    def run_netting_for_rtf(self, _os=None, _excludeOrderMeasure=None):
        """
        mainFunction: Starting Function.
        Add OrderQuantityNotCoveredByForecast and OrderQuantityCoveredByForecast to order data.
        Add NettedForecastQuantityOutput and ConsumedForecastQuantityOutput to forecast data.
        Iterate over order data to call processOrder Function
        """
        # print(">> run net")
        if self.order_remaining not in self.in_orders.columns:
            self.orderQtyHash = self.in_orders[self.order_qty].to_dict()
        else:
            self.orderQtyHash = self.in_orders[self.order_remaining].to_dict()
        if self.forecast_remaining not in self.in_forecasts.columns:
            self.forecastQtyHash = self.in_forecasts[self.forecast_qty].to_dict()
        else:
            self.forecastQtyHash = self.in_forecasts[self.forecast_remaining].to_dict()

        # PROCESS ORDER.
        if _os is not None:
            self.plugin_log(
                f"Run Netting For ({self.order_qty}"
                f": {len(self.in_orders[(self.in_orders[self.order_qty].values > 0) & (self.in_orders[self.order_type] == _os)])} "
                f":: {self.forecast_qty}: {len(self.in_forecasts[self.in_forecasts[self.forecast_qty].values > 0])})"
            )
            self.in_orders[
                (self.in_orders[self.order_qty].values > 0)
                & (self.in_orders[self.order_type] == _os)
                ].apply(
                lambda _x: self.process_order(
                    _x.to_dict(), _x.name, _excludeOrderMeasure
                ),
                axis=1,
            )
        else:
            self.plugin_log(
                f"Run Netting For ({self.order_qty}"
                f": {len(self.in_orders[(self.in_orders[self.order_qty].values > 0)])} "
                f":: {self.forecast_qty}: {len(self.in_forecasts[self.in_forecasts[self.forecast_qty].values > 0])})"
            )
            self.in_orders[(self.in_orders[self.order_qty].values > 0)].apply(
                lambda _x: self.process_order(
                    _x.to_dict(), _x.name, _excludeOrderMeasure
                ),
                axis=1,
            )

        self.in_orders[self.order_remaining] = Series(self.orderQtyHash)
        self.in_forecasts[self.forecast_remaining] = Series(self.forecastQtyHash)
        self.in_orders[self.order_consumed] = (
                self.in_orders[self.order_qty].values
                - self.in_orders[self.order_remaining].values
        )
        self.in_forecasts[self.forecast_consumed] = (
                self.in_forecasts[self.forecast_qty].values
                - self.in_forecasts[self.forecast_remaining].values
        )

    def process_order(self, _orderData, _orderIndex, _excludeOrder=None):
        if _excludeOrder is not None and _orderData[_excludeOrder]:
            return
        oid = _orderData[self.DEMAND_ID]
        if oid in self.default_demand_ids:
            oItem = _orderData[self.f_item]
            oLoc = _orderData[self.f_location]
            oSales = _orderData[self.f_customer]
            oTime = _orderData[self.f_time]
        else:
            oItem = _orderData[self.ITEM]
            oLoc = _orderData[self.LOCATION]
            oSales = _orderData[self.CUSTOMER]
            oTime = _orderData[self.TIME]
        backward = int(_orderData[self.BACKWARD_BUCKETS])
        forward = int(_orderData[self.FORWARD_BUCKETS])
        # print("self.ITEM", self.ITEM)
        # print("oItem", oItem)
        # print((oItem, oLoc, oSales, oTime, backward, forward))
        if not self.use_order_forecast_map:
            consumableTuples = self.order_consumption_tuples.get(
                (oItem, oLoc, oSales, oTime, backward, forward), None
            )

            if consumableTuples is not None:
                self.consume_from_tuples(consumableTuples, _orderIndex)
        else:
            fTime = _orderData[self.f_time]
            forecastIndices = []
            for associatedForecast in self.order_forecast_map_hash.get(
                    (
                            _orderData[self.f_item],
                            _orderData[self.f_location],
                            _orderData[self.f_customer],
                    ),
                    [],
            ):
                try:
                    tmp = self.forecastToIndexMap[
                        (
                            associatedForecast[0],
                            associatedForecast[1],
                            associatedForecast[2],
                        )
                    ][fTime]
                    forecastIndices.append(tmp)
                except KeyError:
                    pass
                if backward > 0:
                    for backTime in self.get_backward_time(fTime, backward):
                        try:
                            tmp = self.forecastToIndexMap[
                                (
                                    associatedForecast[0],
                                    associatedForecast[1],
                                    associatedForecast[2],
                                )
                            ][backTime]
                            forecastIndices.append(tmp)
                        except KeyError:
                            pass
                if forward > 0:
                    for forTime in self.get_forward_time(fTime, forward):
                        try:
                            tmp = self.forecastToIndexMap[
                                (
                                    associatedForecast[0],
                                    associatedForecast[1],
                                    associatedForecast[2],
                                )
                            ][forTime]
                            forecastIndices.append(tmp)
                        except KeyError:
                            pass
            if self.pegging_flag:
                self.consume_from_forecast_index_with_pegging(
                    _orderIndex, forecastIndices
                )
            else:
                self.consume_from_forecast_index(_orderIndex, forecastIndices)

    def consume_from_tuples(self, _consumableTuples, _orderIndex):
        """
        Check for the tuple in the forecastToIndexMap, if we have the index then consume it.
        :param _consumableTuples: List of consumable tuples
        :param _orderIndex: Order index
        :return:
        """
        # print(">> consume_from_tuples")
        for data in _consumableTuples:
            try:
                tmp = self.forecastToIndexMap[data[0:3]][data[3]]
                if self.pegging_flag:
                    isOrderQtyFullConsumed = (
                        self.consume_from_forecast_index_with_pegging(
                            _orderIndex, [tmp]
                        )
                    )
                else:
                    isOrderQtyFullConsumed = self.consume_from_forecast_index(
                        _orderIndex, [tmp]
                    )
                if isOrderQtyFullConsumed:
                    return
            except KeyError:
                # print("ERROR")
                pass

    def consume_from_forecast_index(self, _orderIndex, _forecastIndex):
        for forecastIn in _forecastIndex:
            if forecastIn in self.empty_forecast_indices:
                continue
            curOrderQtyPending = self.orderQtyHash[_orderIndex]
            curForecastAvailable = self.forecastQtyHash[forecastIn]
            # print("curOrderQtyPending", curOrderQtyPending)
            # print("curForecastAvailable", curForecastAvailable)
            if curForecastAvailable == 0:
                self.empty_forecast_indices[forecastIn] = 0
            elif curOrderQtyPending == 0:
                return True
            # CONSUME FORECAST FOR ORDER.
            elif curForecastAvailable > 0 and curOrderQtyPending > 0:
                consume = min(curOrderQtyPending, curForecastAvailable)
                curOrderQtyPending -= consume
                curForecastAvailable -= consume
                self.orderQtyHash[_orderIndex] = curOrderQtyPending
                self.forecastQtyHash[forecastIn] = curForecastAvailable
                if curForecastAvailable == 0:
                    self.empty_forecast_indices[forecastIn] = 0
                if curOrderQtyPending == 0:
                    return True
        return False

    def consume_from_forecast_index_with_pegging(
            self, _orderIndex, _forecastIndex
    ) -> bool:
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>")
        for forecastIn in _forecastIndex:
            if forecastIn in self.empty_forecast_indices:
                continue
            # print(self.forecastQtyHash)
            curOrderQtyPending = self.orderQtyHash[_orderIndex]
            curForecastAvailable = self.forecastQtyHash[forecastIn]
            if curForecastAvailable == 0:
                self.empty_forecast_indices[forecastIn] = 0
            elif curOrderQtyPending == 0:
                return True
            # CONSUME FORECAST FOR ORDER.
            elif curForecastAvailable > 0 and curOrderQtyPending > 0:
                consume = min(curOrderQtyPending, curForecastAvailable)
                curOrderQtyPending -= consume
                curForecastAvailable -= consume
                self.orderQtyHash[_orderIndex] = curOrderQtyPending
                self.forecastQtyHash[forecastIn] = curForecastAvailable
                self.pegging.append(
                    {
                        self.peg_forecast_index: forecastIn,
                        self.peg_order_index: _orderIndex,
                        self.peg_qty_consumed: consume,
                    }
                )
                if curForecastAvailable == 0:
                    self.empty_forecast_indices[forecastIn] = 0
                if curOrderQtyPending == 0:
                    return True
        return False

    def combine_order_with_forecast(self):
        final_order = self.in_orders

        final_grain = [x for x in self.in_orders.columns if ".[" in x] + [
            self.open_order_qty,
            self.order_priority,
            self.FORWARD_BUCKETS,
            self.BACKWARD_BUCKETS,
            self.UPWARD_ITEM,
            self.UPWARD_LOCATION,
            self.UPWARD_CUSTOMER,
            self.UPWARD_TIME,
            self.EXCLUDE_PLANNING,
            self.order_type,
            self.order_consumed_by_all_forecast,
            self.remaining_order_after_forecast,
        ]

        final_grain = final_grain + list(self.orders_seen)

        if self.use_multi_stream:
            final_grain += list(self.forecasts_seen)

        final_order = final_order[final_grain]
        forecast_data_list = []
        # FORM ONE ORDER DATA BY COMBINING ORDER AND MULTIPLE NETTED FORECASTS.
        for forecastName, details in self.fs_map.items():
            rem_forecast = details[self.fs_forecast_remaining]
            if self.use_aggregate:
                tmp_forecast = self.in_forecasts[
                    self.in_forecasts[f"Net_{forecastName}"].values > 0
                    ]
            else:
                tmp_forecast = self.in_forecasts[
                    self.in_forecasts[forecastName].values > 0
                    ]
            tmp_forecast.rename(
                columns={
                    rem_forecast: self.open_order_qty,
                    self.F_FORWARD_BUCKETS: self.FORWARD_BUCKETS,
                    self.F_BACKWARD_BUCKETS: self.BACKWARD_BUCKETS,
                    self.F_UPWARD_ITEM: self.UPWARD_ITEM,
                    self.F_UPWARD_LOCATION: self.UPWARD_LOCATION,
                    self.F_UPWARD_CUSTOMER: self.UPWARD_CUSTOMER,
                    self.F_UPWARD_TIME: self.UPWARD_TIME,
                    self.F_EXCLUDE_PLANNING: self.EXCLUDE_PLANNING,
                },
                inplace=True,
            )

            tmp_forecast[self.DEMAND_ID] = details[self.fs_demand_id]
            tmp_forecast[self.order_priority] = None
            tmp_forecast[self.order_type] = None
            tmp_forecast[self.order_consumed_by_all_forecast] = None
            tmp_forecast[self.remaining_order_after_forecast] = None

            for _order_col in list(self.orders_seen):
                tmp_forecast[_order_col] = None

            if not details[self.fs_is_rtf]:
                tmp_forecast[self.EXCLUDE_PLANNING] = True
            if self.use_aggregate:
                tmp_forecast[self.open_order_qty] = tmp_forecast[
                    [self.open_order_qty, f"Plan_{forecastName}"]
                ].min(axis=1)
                colsInForecast = tmp_forecast.columns
                for col in final_grain:
                    if col not in colsInForecast:
                        tmp_forecast[col] = None

            forecast_data_list.append(tmp_forecast[final_grain])

        final_order = concat([final_order] + forecast_data_list, ignore_index=True)

        final_order.reset_index(inplace=True, drop=True)
        self.in_orders = final_order
        if not self.use_aggregate:
            self.in_orders = self.in_orders[
                self.in_orders[self.EXCLUDE_PLANNING].values == False
                ]

    def net_order_against_rtf(self):
        self.order_qty = self.open_order_qty
        self.forecast_consumed = "CONSUME_RTF"
        self.forecast_remaining = "REMAINING_RTF"
        self.order_remaining = self.orderNotCoverByRTF
        self.order_consumed = self.orderCoverByRTF

        if self.in_RTFs.empty:
            self.in_orders[self.order_consumed] = 0
            self.in_orders[self.order_remaining] = self.in_orders[self.order_qty]
            return
        original_order_grain = []
        # if self.use_aggregate:
        #     original_order_grain = [self.ITEM, self.LOCATION, self.CUSTOMER, self.TIME]
        #     self.ITEM = self.f_item
        #     self.LOCATION = self.f_location
        #     self.CUSTOMER = self.f_customer
        #     self.TIME = self.f_time

        self.forecastToIndexMap = {}
        self.setup_rtf()
        if self.use_order_forecast_map:
            tmpGraph = self.in_orderForecastMapGraph[
                self.in_orderForecastMapGraph[self.graph_rtf_association].values == 1
                ]
            tmpGraph.sort_values(
                by=[
                    self.from_item,
                    self.from_location,
                    self.from_customer,
                    self.graph_priority,
                ],
                inplace=True,
            )
            self.order_forecast_map_hash = {}
            vectorize(self.create_order_forecast_map_hash, otypes=[str])(
                tmpGraph[self.from_item],
                tmpGraph[self.from_location],
                tmpGraph[self.from_customer],
                tmpGraph[self.to_item],
                tmpGraph[self.to_location],
                tmpGraph[self.to_customer],
                self.order_forecast_map_hash,
            )
        self.in_orders[self.EXCLUDE_PLANNING].fillna(value=False, inplace=True)
        self.set_order_priority(_isForecast=True)
        self.empty_forecast_indices = {}
        self.pegging = []
        self.in_forecasts = self.in_RTFs
        self.forecast_qty = self.rtf_qty
        self.run_netting_for_rtf(_excludeOrderMeasure=self.EXCLUDE_PLANNING)
        if self.use_aggregate:
            original_order_grain = [self.ITEM, self.LOCATION, self.CUSTOMER, self.TIME]
            self.ITEM = self.f_item
            self.LOCATION = self.f_location
            self.CUSTOMER = self.f_customer
            self.TIME = self.f_time
        if self.pegging_flag:
            self.append_to_final_pegging(self.pegging)
        if self.use_aggregate:
            (
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.TIME,
            ) = original_order_grain

    def setup_rtf(self):
        self.plugin_log("Cleaning RTF Data.")
        self.in_RTFs = self.in_RTFs[self.in_RTFs[self.rtf_qty].values > 0]
        # SORT FORECAST BY ITS GRAIN
        self.in_RTFs[self.forecast_grain] = self.in_RTFs[self.forecast_grain].astype(
            str
        )
        if self.use_aggregate:
            self.in_RTFs = self.in_RTFs.groupby(self.forecast_grain, as_index=False)[
                self.rtf_qty
            ].sum()
        self.in_RTFs.sort_values(by=sorted(self.forecast_grain), inplace=True)
        self.in_RTFs.reset_index(inplace=True, drop=True)

        if len(self.in_RTFs) > 0:
            if self.pegging_flag:
                vectorize(self.create_forecast_hash_with_pegging, otypes=[str])(
                    self.in_RTFs[self.f_item],
                    self.in_RTFs[self.f_location],
                    self.in_RTFs[self.f_customer],
                    self.in_RTFs[self.f_time],
                    self.in_RTFs.index,
                    self.forecastToIndexMap,
                    self.forecastIndexForPeggingMap,
                )
            else:
                vectorize(self.create_forecast_hash, otypes=[str])(
                    self.in_RTFs[self.f_item],
                    self.in_RTFs[self.f_location],
                    self.in_RTFs[self.f_customer],
                    self.in_RTFs[self.f_time],
                    self.in_RTFs.index,
                    self.forecastToIndexMap,
                )

    def append_to_final_pegging(self, _pegging: list):
        # print("---------------------------------------")
        # print(self.peg_from_demand_id)
        # print(self.peg_from_item)
        # print(self.peg_from_location)
        # print(self.peg_from_customer)
        # print(self.peg_to_item)
        # print(self.peg_to_location)
        # print(self.peg_to_customer)
        # print("---------------------------------------")
        demandIDIndexHash = self.in_orders[self.DEMAND_ID].to_dict()
        orderHash = self.in_orders.set_index(self.in_orders.index)[
            [
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.TIME,
            ]
        ].to_dict()
        for peg in _pegging:
            fromDemandID = demandIDIndexHash.get(peg[self.peg_order_index], None)
            self.finalPegging[self.peg_from_demand_id].append(fromDemandID)
            self.finalPegging[self.peg_from_item].append(
                orderHash[self.ITEM][peg[self.peg_order_index]]
            )
            self.finalPegging[self.peg_from_location].append(
                orderHash[self.LOCATION][peg[self.peg_order_index]]
            )
            self.finalPegging[self.peg_from_customer].append(
                orderHash[self.CUSTOMER][peg[self.peg_order_index]]
            )
            self.finalPegging[self.peg_from_time].append(
                orderHash[self.TIME][peg[self.peg_order_index]]
            )
            pegItem, pegLoc, pegSale, pegTime = self.forecastIndexForPeggingMap.get(
                peg[self.peg_forecast_index], (None, None, None, None)
            )
            self.finalPegging[self.peg_to_item].append(pegItem)
            self.finalPegging[self.peg_to_location].append(pegLoc)
            self.finalPegging[self.peg_to_customer].append(pegSale)
            self.finalPegging[self.peg_to_time].append(pegTime)
            self.finalPegging[self.peg_forecast_measure].append(self.forecast_qty)
            self.finalPegging[self.peg_qty_consumed].append(peg[self.peg_qty_consumed])

    def convert_to_telescopic(
            self, _order: DataFrame, _forecast: DataFrame
    ) -> (DataFrame, DataFrame):
        self.plugin_log("Converting to Telescopic Time.")
        finalOrderHeaders = [
            self.VERSION,
            self.ITEM,
            self.LOCATION,
            self.CUSTOMER,
            self.TelescopicHeader,
            self.DEMAND_ID,
            self.DEMAND_TYPE,
            self.netted_demand_qty,
        ]
        finalForecastHeaders = [
            self.VERSION,
            self.f_item,
            self.f_location,
            self.f_customer,
            self.TelescopicHeader,
            self.DEMAND_ID,
            self.DEMAND_TYPE,
            self.netted_demand_qty,
        ]

        telescopic_cols = list(self.in_telescopic.columns)
        if (
                self.in_telescopic.empty
                or (self.TelescopicHeader not in telescopic_cols)
                or (self.f_time not in telescopic_cols)
        ):
            self.plugin_log(
                "Telescopic input table is incorrect, Not converting to telescopic time grain",
                _type="warn",
            )
            return _order, _forecast

        _order = _order.merge(
            self.in_telescopic[[self.TIME, self.TelescopicHeader]],
            on=[self.TIME],
            how="left",
        )

        _forecast = _forecast.merge(
            self.in_telescopic[[self.f_time, self.TelescopicHeader]],
            on=[self.f_time],
            how="left",
        )
        OrderDemandTypeOutput = _order[finalOrderHeaders]
        ForecastDemandTypeOutput = _forecast[finalForecastHeaders]
        ForecastDemandTypeOutput = ForecastDemandTypeOutput.groupby(
            [
                self.VERSION,
                self.f_item,
                self.f_location,
                self.f_customer,
                self.DEMAND_ID,
                self.TelescopicHeader,
                self.DEMAND_TYPE,
            ],
            as_index=False,
            observed=True,
        ).agg({self.netted_demand_qty: "sum"})
        return OrderDemandTypeOutput, ForecastDemandTypeOutput

    def get_pegging_data(self):
        pegging = DataFrame.from_dict(self.finalPegging)
        if self.pegging_flag:
            if (
                    len(pegging) == 0
            ):  # If pegging df is empty, just create an empty dataframe with the required columns
                pegging = DataFrame(
                    columns=[
                        self.VERSION,
                        self.peg_seq,
                        self.peg_from_demand_id,
                        self.peg_from_item,
                        self.peg_from_location,
                        self.peg_from_customer,
                        self.peg_from_time,
                        self.peg_to_item,
                        self.peg_to_location,
                        self.peg_to_customer,
                        self.peg_to_time,
                        self.peg_qty_consumed,
                        self.peg_forecast_measure,
                    ]
                )
            else:
                pegCols = [
                    self.peg_from_demand_id,
                    self.peg_from_item,
                    self.peg_from_location,
                    self.peg_from_customer,
                    self.peg_from_time,
                    self.peg_to_item,
                    self.peg_to_location,
                    self.peg_to_customer,
                    self.peg_to_time,
                ]
                pegging.insert(0, self.peg_seq, 0)
                pegging[self.peg_seq] = pegging.groupby(by=pegCols).cumcount().add(1)
                pegging.insert(0, self.VERSION, self.curVersion)
                pegging[self.peg_seq] = pegging[self.peg_seq].astype(int)
        else:
            pegging = DataFrame(
                columns=[
                    self.VERSION,
                    self.peg_seq,
                    self.peg_from_demand_id,
                    self.peg_from_item,
                    self.peg_from_location,
                    self.peg_from_customer,
                    self.peg_from_time,
                    self.peg_to_item,
                    self.peg_to_location,
                    self.peg_to_customer,
                    self.peg_to_time,
                    self.peg_qty_consumed,
                    self.peg_forecast_measure,
                ]
            )
        return pegging

    def to_key_col(self, s: str):
        s = str(s)
        s_i = s.find("[")
        e_i = s.find("]")
        content = s[s_i + 1: e_i]
        content = content.replace(" ", "") + "Key"
        return f"Time.[{content}]"

    def get_aggregate_grains(self):
        self.plugin_log(f"Get Aggregate Grains.")
        excludeMeasures = [
            self.F_BACKWARD_BUCKETS,
            self.F_FORWARD_BUCKETS,
            self.F_UPWARD_ITEM,
            self.F_UPWARD_LOCATION,
            self.F_UPWARD_CUSTOMER,
            self.F_UPWARD_TIME,
            self.F_EXCLUDE_NETTING,
            self.F_EXCLUDE_PLANNING,
        ]
        forecastMeasureList: list = [
            col
            for col in self.in_forecasts.columns
            if "." not in col and col not in excludeMeasures
        ]
        # Get Aggregation Grains
        try:
            agg_grain = self.parameters[Config.DN_AGGREGATE_LEVELS].split(",")
            agg_item, agg_location, agg_customer, agg_time = agg_grain
        except KeyError:
            agg_item, agg_location, agg_customer, agg_time = self.forecast_grain

        if "key" not in agg_time:
            agg_time = self.to_key_col(agg_time)

        # Add higher grain data to Forecast & RTF
        if agg_item != self.f_item:
            self.in_forecasts = self.in_forecasts.merge(
                self.master_item[[agg_item, self.f_item]],
                on=self.f_item,
                how="left",
            )
            self.in_RTFs = self.in_RTFs.merge(
                self.master_item[[agg_item, self.f_item]],
                on=self.f_item,
                how="left",
            )
            self.f_item = agg_item
        if agg_location != self.f_location:
            self.in_forecasts = self.in_forecasts.merge(
                self.master_location[[agg_location, self.f_location]],
                on=self.f_location,
                how="left",
            )
            self.in_RTFs = self.in_RTFs.merge(
                self.master_location[[agg_location, self.f_location]],
                on=self.f_location,
                how="left",
            )
            self.f_location = agg_location
        if agg_customer != self.f_customer:
            self.in_forecasts = self.in_forecasts.merge(
                self.master_customer[[agg_customer, self.f_customer]],
                on=self.f_customer,
                how="left",
            )
            self.in_RTFs = self.in_RTFs.merge(
                self.master_customer[[agg_customer, self.f_customer]],
                on=self.f_customer,
                how="left",
            )
            self.f_customer = agg_customer
        if agg_time != self.f_time:
            self.in_forecasts = self.in_forecasts.merge(
                self.master_time[[agg_time, self.f_time]],
                on=self.f_time,
                how="left",
            )
            self.in_RTFs = self.in_RTFs.merge(
                self.master_time[[agg_time, self.f_time]],
                on=self.f_time,
                how="left",
            )
            self.f_time = agg_time
        self.forecast_grain = [
            self.f_item,
            self.f_location,
            self.f_customer,
            self.f_time,
        ]

        self.in_forecasts.drop_duplicates(inplace=True)
        self.in_RTFs.drop_duplicates(inplace=True)

        # Add Net & Plan Forecast Qty for each forecast.
        netPlanMeasureList = []
        for forecast in forecastMeasureList:
            netForecast = f"Net_{forecast}"
            planForecast = f"Plan_{forecast}"
            self.in_forecasts[netForecast] = None
            self.in_forecasts.loc[
                ~(self.in_forecasts[self.F_EXCLUDE_NETTING]), netForecast
            ] = self.in_forecasts[forecast]
            self.in_forecasts.loc[
                ~(self.in_forecasts[self.F_EXCLUDE_PLANNING]), planForecast
            ] = self.in_forecasts[forecast]
            netPlanMeasureList.append(netForecast)
            netPlanMeasureList.append(planForecast)
        # Aggregate the Forecast Data to Aggregate Level.
        aggregationHash = {column: "sum" for column in netPlanMeasureList}
        aggregationHash.update(
            {self.F_BACKWARD_BUCKETS: "mean", self.F_FORWARD_BUCKETS: "mean"}
        )
        self.original_in_forecast = self.in_forecasts.copy(deep=True)
        self.original_in_forecast[netPlanMeasureList] = self.original_in_forecast[netPlanMeasureList].fillna(0)
        grouping_columns = [self.VERSION] + self.forecast_grain
        self.in_forecasts[grouping_columns] = self.in_forecasts[
            grouping_columns
        ].astype(str)
        self.in_forecasts = self.in_forecasts.groupby(
            by=grouping_columns,
            as_index=False,
        ).agg(aggregationHash)
        # Upward Buckets are not needed due to Aggregate Netting.
        self.in_orders[self.UPWARD_ITEM] = 0
        self.in_orders[self.UPWARD_LOCATION] = 0
        self.in_orders[self.UPWARD_CUSTOMER] = 0
        self.in_orders[self.UPWARD_TIME] = 0
        self.in_forecasts[self.F_UPWARD_ITEM] = 0
        self.in_forecasts[self.F_UPWARD_LOCATION] = 0
        self.in_forecasts[self.F_UPWARD_CUSTOMER] = 0
        self.in_forecasts[self.F_UPWARD_TIME] = 0
        self.in_forecasts[self.F_EXCLUDE_PLANNING] = False
        self.in_forecasts[self.F_EXCLUDE_NETTING] = False
        # print(self.in_forecasts.columns)

    def add_forecast_grains_to_order(self):
        """addForecastGrainsToOrder: Change order data grain to forecast data grain."""
        if self.ITEM != self.f_item:
            self.in_orders = merge(
                self.in_orders,
                self.master_item[[self.ITEM, self.f_item]],
                on=self.ITEM,
                how="left",
            )
        if self.LOCATION != self.f_location:
            self.in_orders = merge(
                self.in_orders,
                self.master_location[[self.LOCATION, self.f_location]],
                on=self.LOCATION,
                how="left",
            )
        if self.CUSTOMER != self.f_customer:
            self.in_orders = merge(
                self.in_orders,
                self.master_customer[[self.CUSTOMER, self.f_customer]],
                on=self.CUSTOMER,
                how="left",
            )
        if self.TIME != self.f_time:
            self.in_orders = merge(
                self.in_orders,
                self.master_time[[self.TIME, self.f_time]],
                on=self.TIME,
                how="left",
            )

        self.in_orders.drop_duplicates(inplace=True)

    def generate_stream_hash(self):
        if len(self.in_orderStreamParameters) > 0 and self.use_multi_stream:
            self.in_orderStreamParameters.sort_values(
                by=[self.os_cons_seq], inplace=True
            )
            self.in_orderStreamParameters.apply(
                lambda _os: self.populate_order_stream_hash(_os.to_dict()), axis=1
            )
        else:
            order_measure_without_space = self.order_qty.replace(" ", "")
            consumedOrder = f"consumed_{order_measure_without_space}"
            remainingOrder = f"remaining_{order_measure_without_space}"
            self.os_map[self.order_qty] = [
                {
                    self.os_forecast: self.forecast_qty,
                    self.os_is_rtf: True,
                    self.os_com_order: Config.COM_ORDER,
                    self.os_new_order: Config.NEW_ORDER,
                    self.os_unf_order: Config.UNF_ORDER,
                    self.os_past_order_dt: Config.UNF_ORDER,
                    self.os_order_consumed: consumedOrder,
                    self.os_order_remaining: remainingOrder,
                }
            ]
            self.in_orders[self.order_type] = self.order_qty
        if len(self.in_forecastStreamParameters) > 0 and self.use_multi_stream:
            self.in_forecastStreamParameters.sort_values(by=[self.fs_seq], inplace=True)
            self.in_forecastStreamParameters.apply(
                lambda _fs: self.populate_forecast_stream_hash(_fs.to_dict()),
                axis=1,
            )
        else:
            forecast_stream_without_space = self.forecast_qty.replace(" ", "")
            consumedForecast = f"consumed_{forecast_stream_without_space}"
            remainingForecast = f"remaining_{forecast_stream_without_space}"
            self.fs_map[self.forecast_qty] = {
                self.fs_demand_id: Config.FORECAST_DEMAND_ID,
                self.fs_is_rtf: True,
                self.fs_com_forecast: Config.COM_FCST,
                self.fs_new_forecast: Config.NEW_FCST,
                self.fs_forecast_consumed: consumedForecast,
                self.fs_forecast_remaining: remainingForecast,
                self.is_base: True,
            }

    def net_order_against_agg_forecast(self):
        self.generate_stream_hash()
        for os, os_detail in self.os_map.items():
            os_detail = os_detail[0]
            self.order_consumed = os_detail[self.os_order_consumed]
            self.order_remaining = os_detail[self.os_order_remaining]
            self.forecast_qty = os_detail[self.os_forecast]
            fs_detail = self.fs_map[self.forecast_qty]
            self.forecast_qty = f"Net_{self.forecast_qty}"
            self.forecast_consumed = fs_detail[self.fs_forecast_consumed]
            self.forecast_remaining = fs_detail[self.fs_forecast_remaining]
            self.in_forecasts[self.forecast_qty].fillna(0, inplace=True)
            self.empty_forecast_indices = {}
            self.pegging = []
            self.net_order_from_native(fs_detail[self.is_base])
            self.run_netting(os, _excludeOrderMeasure=self.EXCLUDE_NETTING)
            self.orders_seen.add(self.order_consumed)
            self.orders_seen.add(self.order_remaining)
            self.forecasts_seen.add(self.forecast_consumed)
            if self.pegging_flag:
                self.append_to_final_pegging(self.pegging)

        self.in_orders[self.remaining_order_after_forecast] = Series(self.orderQtyHash)
        self.in_orders[self.order_consumed_by_all_forecast] = (
                self.in_orders[self.order_qty]
                - self.in_orders[self.remaining_order_after_forecast]
        )

        self.in_orders[self.remaining_order_after_forecast] = (
                self.in_orders[self.open_order_qty]
                - self.in_orders[self.order_consumed_by_all_forecast]
        )

        self.in_orders.loc[
            self.in_orders[self.remaining_order_after_forecast] < 0,
            self.remaining_order_after_forecast,
        ] = 0

    def net_order_from_native(self, _isBaseForecast):
        # Get all Qty in hash form to Lookup
        if self.order_remaining not in self.in_orders.columns:
            self.orderQtyHash = self.in_orders[self.order_qty].to_dict()
        else:
            self.orderQtyHash = self.in_orders[self.order_remaining].to_dict()
        if self.forecast_remaining not in self.in_forecasts.columns:
            self.forecastQtyHash = self.in_forecasts[self.forecast_qty].to_dict()
        else:
            self.forecastQtyHash = self.in_forecasts[self.forecast_remaining].to_dict()
        self.originalForecastQtyHash = self.original_in_forecast[
            self.forecast_qty
        ].to_dict()

        vectorize(self.consume_from_native, otypes=[str])(
            self.in_orders[self.ITEM],
            self.in_orders[self.LOCATION],
            self.in_orders[self.CUSTOMER],
            self.in_orders[self.TIME],
            self.in_orders[self.f_item],
            self.in_orders[self.f_location],
            self.in_orders[self.f_customer],
            self.in_orders[self.f_time],
            self.in_orders[self.EXCLUDE_NETTING],
            self.in_orders.index,
        )
        if _isBaseForecast & self.ConsumeOnNativeBeforeAggregation:
            self.in_orders[self.order_remaining] = Series(self.orderQtyHash)
            self.in_forecasts[self.forecast_remaining] = Series(self.forecastQtyHash)
        else:
            self.in_orders[self.NATIVE_CONSUME] = Series(self.orderQtyHash)
            self.in_forecasts[self.NATIVE_CONSUME] = Series(self.forecastQtyHash)
        self.original_in_forecast[self.forecast_remaining] = Series(
            self.originalForecastQtyHash
        )

    def consume_from_native(
            self,
            _oItem,
            _oLoc,
            _oSales,
            _oTime,
            _fItem,
            _fLoc,
            _fSales,
            _fTime,
            _exclude,
            _oIndex,
    ):
        if _exclude:
            return
        # Find the Original Forecast Index
        try:
            originalForecastIndex = self.originalForecastToIndexMap[
                (_oItem, _oLoc, _oSales)
            ][_oTime]
        except KeyError:
            return
        # Net Order & Forecast
        forecastQty = self.originalForecastQtyHash[originalForecastIndex]
        orderQty = self.orderQtyHash[_oIndex]
        consume = min(orderQty, forecastQty)
        self.orderQtyHash[_oIndex] -= consume
        self.originalForecastQtyHash[originalForecastIndex] -= consume
        fIndex = self.forecastToIndexMap[(_fItem, _fLoc, _fSales)][_fTime]
        self.forecastQtyHash[fIndex] -= consume

    def separate_past_orders(self):
        if self.in_pastOrderDate.empty:
            self.plugin_log("Past Order Date is Not Available", "warn")
        else:
            past_date = to_datetime(self.in_pastOrderDate[self.TIME][0])
            self.past_orders = self.in_orders[
                to_datetime(self.in_orders[self.TIME]) < past_date
                ]
            self.in_orders = self.in_orders[
                to_datetime(self.in_orders[self.TIME]) >= past_date
                ]


class SkipNetting:
    def __init__(
            self,
            in_orders: DataFrame,
            in_forecasts: DataFrame,
            in_forecastStreamParameters: DataFrame,
            in_parameters: dict,
            logger,
    ):
        self.class_name: str = __name__
        self.class_version: str = "v24.5"
        self.in_orders: DataFrame = in_orders
        self.in_forecasts: DataFrame = in_forecasts
        self.in_forecastStreamParameters: DataFrame = in_forecastStreamParameters
        self.logger = logger

        self.parameters: dict = in_parameters

        self.netted_demand_qty: str = self.parameters.get(
            Config.DN_OUT_QTY, Config.OUT_QTY
        )

        self.order_qty: str = self.parameters.get(Config.DN_ORDER_QTY, Config.ORDER_QTY)
        self.DEMAND_TYPE: str = Config.DEMAND_TYPE
        self.DEMAND_ID: str = Config.DEMAND_ID
        self.fs_stream: str = "Forecast Stream"
        self.fs_demand_id: str = "Forecast Demand ID"
        self.fs_seq: str = "Forecast Netting Sequence"
        self.TIME: str = self.parameters.get(Config.DN_TIME_ATTR, Config.WEEK)
        self.f_time: str = self.parameters.get(Config.DN_TIME_ATTR, Config.WEEK)

        def to_key_col(s: str):
            s = str(s)
            s_i = s.find("[")
            e_i = s.find("]")
            content = s[s_i + 1: e_i]
            content = content.replace(" ", "") + "Key"
            return f"Time.[{content}]"

        self.TIME = to_key_col(self.TIME)
        self.f_time = to_key_col(self.f_time)

        self.open_order_qty: str = self.parameters.get(
            Config.DN_OPEN_ORDER, Config.OPEN_ORDER_QTY
        )
        self.forecast_qty: str = self.parameters.get(
            Config.DN_FORECAST_QTY, Config.FORECAST_QTY
        )
        self.TelescopicHeader: str = self.parameters.get(
            Config.DN_TELESCOPIC_TIME_ATTR, self.TIME
        )

        def string_to_bool(s: str):
            if s.lower() == "true" or s == "1":
                return True
            return False

        self.use_multi_stream: bool = string_to_bool(
            str(
                self.parameters.get(Config.DN_USE_MULTI_STREAM, Config.USE_MULTI_STREAM)
            )
        )

        self.fs_map = {}

    def plugin_log(self, _msg, _type=""):
        # elapsed = time() - self.startTime
        pre = f"{self.class_name}_{self.class_version}"
        if _type == "warn":
            self.logger.warning(f"{pre}: {_msg}")
        elif _type == "error":
            self.logger.error(f"{pre}: {_msg}")
        elif _type == "debug":
            self.logger.debug(f"{pre}: {_msg}")
        else:
            print(f"{pre}: {_msg}")
            self.logger.info(f"{pre}: {_msg}")

    def pre_proc(self):

        dimCol = [_x for _x in list(self.in_orders.columns) if ".[" in _x]
        self.in_orders[dimCol] = self.in_orders[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_forecasts.columns) if ".[" in _x]
        self.in_forecasts[dimCol] = self.in_forecasts[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_forecastStreamParameters.columns) if ".[" in _x]
        self.in_forecastStreamParameters[dimCol] = self.in_forecastStreamParameters[dimCol].astype(str)

        return

    def run_skip_netting(self):

        self.pre_proc()
        out_demand_types: DataFrame = DataFrame()
        out_forecast_types: DataFrame = DataFrame()
        order_out_grains = [x for x in self.in_orders.columns if ".[" in x]
        order_out_grains = order_out_grains + [self.DEMAND_TYPE, self.netted_demand_qty]

        # Processing Order
        self.in_orders[self.order_qty].fillna(0, inplace=True)
        self.in_orders[self.open_order_qty].fillna(
            self.in_orders[self.order_qty], inplace=True
        )
        self.in_orders = self.in_orders[self.in_orders[self.order_qty].values > 0]
        self.in_orders = self.in_orders[self.in_orders[self.open_order_qty].values >= 0]

        self.in_orders[self.DEMAND_TYPE] = "Orders"
        self.in_orders[self.netted_demand_qty] = self.in_orders[self.open_order_qty]

        out_demand_types = self.in_orders[order_out_grains]

        # Processing Forecast
        forecast_out_grains = [x for x in self.in_forecasts.columns if ".[" in x]
        forecast_out_grains = forecast_out_grains + [
            self.DEMAND_TYPE,
            self.netted_demand_qty,
        ]
        self.make_fs_map()

        for forecastName, details in self.fs_map.items():
            fs_order = self.in_forecasts[self.in_forecasts[forecastName] > 0]
            fs_order[self.DEMAND_TYPE] = "Forecast"
            fs_order[self.netted_demand_qty] = fs_order[forecastName]
            fs_order = fs_order[forecast_out_grains]
            fs_order[self.DEMAND_ID] = details[self.fs_demand_id]

            out_forecast_types = concat(
                [out_forecast_types, fs_order], ignore_index=True
            )

        out_forecast_types[self.DEMAND_ID].fillna(Config.FORECAST_DEMAND_ID)

        return out_demand_types, out_forecast_types

    def make_fs_map(self):
        if len(self.in_forecastStreamParameters) > 0:
            self.in_forecastStreamParameters.sort_values(by=[self.fs_seq], inplace=True)
            self.in_forecastStreamParameters.apply(
                lambda _fs: self.populate_forecast_stream_hash(_fs.to_dict()),
                axis=1,
            )
        else:
            self.fs_map[self.forecast_qty] = {
                self.fs_demand_id: Config.FORECAST_DEMAND_ID,
            }

    def populate_forecast_stream_hash(self, forecast_stream_param):
        forecast_stream = forecast_stream_param[self.fs_stream]
        if isna(forecast_stream):
            return
        self.fs_map[forecast_stream] = {
            self.fs_demand_id: forecast_stream_param[self.fs_demand_id],
        }


class Profiling:
    def __init__(
            self,
            in_netted_order: DataFrame,
            in_netted_forecast: DataFrame,
            in_basis: DataFrame,
            in_parameters: dict,
            in_telescopic: DataFrame,
            logger,
    ):
        self.class_name: str = __name__
        self.class_version: str = "v24.5"

        self.in_netted_order = in_netted_order
        self.in_netted_forecast = in_netted_forecast
        self.in_basis = in_basis
        self.in_telescopic = in_telescopic
        self.logger = logger

        def to_key_col(s: str):
            s = str(s)
            s_i = s.find("[")
            e_i = s.find("]")
            content = s[s_i + 1: e_i]
            content = content.replace(" ", "") + "Key"
            return f"Time.[{content}]"

        self.parameters: dict = in_parameters

        self.assortment_basis: str = self.parameters.get(
            Config.DN_PROFILED_ASSORTMENT_BASIS, Config.PROFILED_ASSORTMENT_BASIS
        )

        self.basis: str = self.parameters.get(
            Config.DN_PROFILED_BASIS, Config.PROFILED_BASIS
        )

        self.basis_spread_method: str = self.parameters.get(
            Config.DN_OUT_PROFILED_BUCKET, Config.OUT_PROFILED_BUCKET
        )

        self.spread_method: str = self.parameters.get(
            Config.DN_PROFILED_BASIS_SPREAD_METHOD, Config.PROFILED_BASIS_SPREAD_METHOD
        )

        self.netted_demand_qty: str = self.parameters.get(
            Config.DN_OUT_QTY, Config.OUT_QTY
        )

        self.order_due_date: str = self.parameters.get(
            Config.DN_ORDER_DUE_DATE, Config.ORDER_DUE_DATE
        )
        self.final_time_df: DataFrame = DataFrame()

        self.TIME: str = self.parameters.get(Config.DN_TIME_ATTR, Config.WEEK)

        self.final_time_attribute: str = self.parameters.get(
            Config.DN_OUT_FINAL_TIME_ATTR, Config.OUT_FINAL_TIME_ATTR
        )

        self.TelescopicHeader: str = self.parameters.get(
            Config.DN_TELESCOPIC_TIME_ATTR, self.TIME
        )

        self.TIME = to_key_col(self.TIME)
        self.final_time_attribute = to_key_col(self.final_time_attribute)
        self.TelescopicHeader = to_key_col(self.TelescopicHeader)

        self.PARTIAL_WEEK_KEY: str = "Time.[PartialWeekKey]"
        self.DAY_KEY: str = "Time.[DayKey]"

        self.VERSION: str = Config.VERSION
        self.DEMAND_TYPE: str = Config.DEMAND_TYPE
        self.DEMAND_ID: str = Config.DEMAND_ID
        self.ITEM: str = self.parameters.get(Config.DN_ITEM_ATTR, Config.ITEM)
        self.LOCATION: str = self.parameters.get(
            Config.DN_LOCATION_ATTR, Config.LOCATION
        )
        self.CUSTOMER: str = self.parameters.get(
            Config.DN_CUSTOMER_ATTR, Config.CUSTOMER
        )

    def plugin_log(self, _msg, _type=""):
        # elapsed = time() - self.startTime
        pre = f"{self.class_name}_{self.class_version}"
        if _type == "warn":
            self.logger.warning(f"{pre}: {_msg}")
        elif _type == "error":
            self.logger.error(f"{pre}: {_msg}")
        elif _type == "debug":
            self.logger.debug(f"{pre}: {_msg}")
        else:
            print(f"{pre}: {_msg}")
            self.logger.info(f"{pre}: {_msg}")

    def pre_proc(self):

        dimCol = [_x for _x in list(self.in_netted_order.columns) if ".[" in _x]
        self.in_netted_order[dimCol] = self.in_netted_order[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_netted_forecast.columns) if ".[" in _x]
        self.in_netted_forecast[dimCol] = self.in_netted_forecast[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_basis.columns) if ".[" in _x]
        self.in_basis[dimCol] = self.in_basis[dimCol].astype(str)

        dimCol = [_x for _x in list(self.in_telescopic.columns) if ".[" in _x]
        self.in_telescopic[dimCol] = self.in_telescopic[dimCol].astype(str)

        return

    def run_profiling(self):

        self.pre_proc()

        if self.final_time_attribute == self.TIME:
            return self.in_netted_order, self.in_netted_forecast

        self.profile_demand()

        self.spread_forecast_to_partial_week()

        self.convert_to_telescopic()

        if self.final_time_attribute == self.TelescopicHeader:
            return self.in_netted_order, self.in_netted_forecast

        if self.spread_method == "By Basis":

            if self.assortment_basis in list(self.in_basis.columns):
                self.in_basis = self.in_basis[self.in_basis[self.assortment_basis] > 0]

            basis_table_cols = list(self.in_basis.columns)
            netted_table_cols = list(self.in_netted_forecast.columns)

            forecast_cols = list(self.in_netted_forecast.columns)

            common_cols = [col for col in basis_table_cols if col in netted_table_cols]

            if len(common_cols) == 0:
                self.plugin_log(
                    "No common col between netted forecast table and basis table, Skipping the Forecast Profiling !",
                    _type="warn",
                )

                return self.in_netted_order, self.in_netted_forecast

            self.in_netted_forecast = self.in_netted_forecast.merge(
                self.in_basis, on=common_cols, how="inner"
            )

            self.in_netted_forecast = self.in_netted_forecast.groupby(
                by=forecast_cols, as_index=False, observed=True
            ).apply(self.apply_norm)

        else:

            if self.final_time_attribute not in list(self.in_telescopic.columns):
                self.plugin_log(
                    f"No final time attr col: {self.final_time_attribute} in telescopic table, Skipping the Spreading !",
                    _type="warn",
                )

                return self.in_netted_order, self.in_netted_forecast

            self.in_netted_forecast = self.in_netted_forecast.merge(
                self.in_telescopic[[self.TelescopicHeader, self.final_time_attribute]],
                on=[self.TelescopicHeader],
                how="inner",
            )
            self.in_netted_forecast = (
                self.in_netted_forecast.drop_duplicates().reset_index(drop=True)
            )

            forecast_cols = [
                self.VERSION,
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.TelescopicHeader,
                self.DEMAND_ID,
                self.DEMAND_TYPE,
            ]

            if self.spread_method == "Equal Spread":

                self.in_netted_forecast["group_size"] = self.in_netted_forecast.groupby(
                    by=forecast_cols
                )[self.netted_demand_qty].transform("size")

                self.in_netted_forecast[self.netted_demand_qty] = (
                        self.in_netted_forecast[self.netted_demand_qty]
                        / self.in_netted_forecast["group_size"]
                )

            elif self.spread_method == "Last Bucket":

                self.in_netted_forecast = (
                    self.in_netted_forecast.groupby(
                        by=forecast_cols, as_index=False, observed=True
                    )
                    .last()
                    .reset_index(drop=True)
                )

            else:
                # uses first bucket by default
                self.in_netted_forecast = (
                    self.in_netted_forecast.groupby(
                        by=forecast_cols, as_index=False, observed=True
                    )
                    .first()
                    .reset_index(drop=True)
                )

        return self.in_netted_order, self.in_netted_forecast

    def add_date(self, group):

        order_date = to_datetime(group[self.order_due_date].iloc[0])
        temp_time_df = self.final_time_df[
            to_datetime(self.final_time_df[self.final_time_attribute]) <= order_date
            ]
        group[self.final_time_attribute] = to_datetime(
            temp_time_df[self.final_time_attribute]
        ).max()

        return group

    def profile_demand(self):
        if self.in_netted_order.empty:
            self.in_netted_order[self.final_time_attribute] = self.in_netted_order[self.TIME]
            return
        if self.final_time_attribute in list(self.in_telescopic.columns):

            self.final_time_df = self.in_telescopic[[self.final_time_attribute]]
            max_order_date = (
                to_datetime(self.in_netted_order[self.order_due_date])
            ).max()
            self.final_time_df = self.final_time_df[
                to_datetime(self.final_time_df[self.final_time_attribute])
                <= max_order_date
                ]

            order_due_dates_df = self.in_netted_order[[self.order_due_date]]
            order_due_dates_df = order_due_dates_df.drop_duplicates().reset_index(
                drop=True
            )

            order_due_dates_df = order_due_dates_df.groupby(
                by=[self.order_due_date], as_index=False, observed=True
            ).apply(self.add_date)

            self.in_netted_order = self.in_netted_order.merge(
                order_due_dates_df, on=[self.order_due_date], how="left"
            )

        else:
            self.plugin_log(
                f"{self.final_time_attribute} not in master time, skipping the Profiling !",
                _type="warn",
            )

            return self.in_netted_order, self.in_netted_forecast

    def spread_forecast_to_partial_week(self):
        # calculate partial week basis
        partial_week_day_df = self.in_telescopic[[self.PARTIAL_WEEK_KEY, self.DAY_KEY]]
        partial_week_day_df = partial_week_day_df.drop_duplicates().reset_index(
            drop=True
        )

        partial_week_day_df["basis"] = partial_week_day_df.groupby(
            by=[self.PARTIAL_WEEK_KEY]
        )[self.DAY_KEY].transform("size")

        partial_week_day_df.drop(columns=[self.DAY_KEY], inplace=True)
        partial_week_day_df = partial_week_day_df.drop_duplicates().reset_index(
            drop=True
        )

        # spread forecast by partial week basis

        original_forecast_grains = list(self.in_netted_forecast.columns)

        self.in_netted_forecast = self.in_netted_forecast.merge(
            self.in_telescopic[[self.TIME, self.PARTIAL_WEEK_KEY]],
            on=[self.TIME],
            how="inner",
        )
        self.in_netted_forecast = self.in_netted_forecast.merge(
            partial_week_day_df, on=[self.PARTIAL_WEEK_KEY], how="inner"
        )

        self.in_netted_forecast = self.in_netted_forecast.drop_duplicates().reset_index(
            drop=True
        )
        self.in_netted_forecast["total"] = self.in_netted_forecast.groupby(
            by=original_forecast_grains
        )["basis"].transform("sum")
        self.in_netted_forecast["ratio"] = (
                self.in_netted_forecast["basis"] / self.in_netted_forecast["total"]
        )
        self.in_netted_forecast[self.netted_demand_qty] = (
                self.in_netted_forecast[self.netted_demand_qty]
                * self.in_netted_forecast["ratio"]
        )

        self.in_netted_forecast.drop(columns=["basis", "total", "ratio"], inplace=True)

    def convert_to_telescopic(self):
        self.in_netted_forecast = self.in_netted_forecast.merge(
            self.in_telescopic[[self.PARTIAL_WEEK_KEY, self.TelescopicHeader]],
            on=[self.PARTIAL_WEEK_KEY],
            how="left",
        )
        self.in_netted_forecast = self.in_netted_forecast.drop_duplicates().reset_index(
            drop=True
        )

        finalForecastHeaders = [
            self.VERSION,
            self.ITEM,
            self.LOCATION,
            self.CUSTOMER,
            self.TelescopicHeader,
            self.DEMAND_ID,
            self.DEMAND_TYPE,
            self.netted_demand_qty,
        ]

        self.in_netted_forecast = self.in_netted_forecast[finalForecastHeaders]

        self.in_netted_forecast = self.in_netted_forecast.groupby(
            [
                self.VERSION,
                self.ITEM,
                self.LOCATION,
                self.CUSTOMER,
                self.DEMAND_ID,
                self.TelescopicHeader,
                self.DEMAND_TYPE,
            ],
            as_index=False,
            observed=True,
        ).agg({self.netted_demand_qty: "sum"})

    def apply_norm(self, group):

        group["total"] = group[self.basis].sum()
        group["ratio"] = group[self.basis] / group["total"]

        if self.basis_spread_method == "Distribute":
            group.sort_values(by=["ratio"], inplace=True, ignore_index=True)
            sum_until_now = 0
            total = group.loc[0, self.netted_demand_qty]
            ratio_of_rem_rows = 1.000000
            for index, row in group.iterrows():
                dist_int = floor(
                    ((total - sum_until_now) * row["ratio"]) / ratio_of_rem_rows
                )
                group.loc[index, self.netted_demand_qty] = dist_int
                sum_until_now += dist_int
                ratio_of_rem_rows -= row["ratio"]
            return group

        group[self.netted_demand_qty] = group[self.netted_demand_qty] * group["ratio"]
        if self.basis_spread_method == "Round Up":
            group[self.netted_demand_qty] = ceil(group[self.netted_demand_qty])
        elif self.basis_spread_method == "Round Down":
            group[self.netted_demand_qty] = floor(group[self.netted_demand_qty])
        return group
