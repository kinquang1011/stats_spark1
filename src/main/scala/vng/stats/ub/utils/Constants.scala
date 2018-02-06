package vng.stats.ub.utils

object Constants {

    val ID = "id"
    val USER = "spark"

    val TIMING = "timing"
    val A1 = "a1"
    val A7 = "a7"
    val A30 = "a30"
    val A60 = "a60"
    val AC30 = "monthly"
    val AC60 = "2monthly"

    val KPI = "kpi"
    val DAILY = "daily"
    val WEEKLY = "weekly"
    val MONTHLY = "monthly"

    val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"

    val HOME_DIR = "/ge/warehouse"
    val GAMELOG_DIR = "/ge/gamelogs"
    val WAREHOUSE_DIR = "/ge/warehouse"

    val DEVICE_TYPE = "device"
    val ROLE_REGISTER_TYPE = "role_register"
    val LOGIN_LOGOUT_TYPE = "login_logout"
    val ACC_REGISTER_TYPE = "acc_register"
    val SERVER_ACC_REGISTER_TYPE = "server_acc_register"
    val PAYMENT_TYPE = "payment"
    val FIRST_CHARGE_TYPE = "first_charge"
    val TOTAL_LOGIN_ACC_TYPE = "total_login_acc"
    val TOTAL_PAID_ACC_TYPE = "total_paid_acc"
    val CCU_TYPE = "ccu"
    val MONEY_FLOW_TYPE = "money_flow"
    val CHARACTER_INFO_TYPE = "character_info"
    val COUNTRY_MAPPING_TYPE = "country_mapping"
    val NEW_LOGIN_DEVICE_TYPE = "new_login_device"
    val NEW_PAID_DEVICE_TYPE = "new_paid_device"

    val RAWLOG_FORMAT_JSON = "json"
    val RAWLOG_FORMAT_TSV = "tsv"

    val DATA_TYPE_STRING = "string"
    val DATA_TYPE_INTEGER = "int"
    val DATA_TYPE_LONG = "long"
    val DATA_TYPE_DOUBLE = "double"
    val DATA_EMPTY_STRING = "''"

    val ENUM0 = 0
    val ENUM1 = 1

    object PARQUET {
        val DEVICE_OUTPUT_FOLDER = "device"
        val ACC_REGISTER_OUTPUT_FOLDER = "accregister"
        val FIRST_CHARGE_OUTPUT_FOLDER = "first_charge"
        val ROLE_REGISTER_OUTPUT_FOLDER = "roleregister"
        val LOGIN_LOGOUT_OUTPUT_FOLDER = "loginlogout"
        val PAYMENT_OUTPUT_FOLDER = "payment"
        val TOTAL_ACC_LOGIN_OUTPUT_FOLDER = "total_login_acc"
        val TOTAL_ACC_PAID_OUTPUT_FOLDER = "total_paid_acc"
        val CCU_OUTPUT_FOLDER = "ccu"
    }

    object PARQUET_2 {
        val DEVICE_OUTPUT_FOLDER = "device"
        val ACC_REGISTER_OUTPUT_FOLDER = "accregister_2"
        val DEVICE_REGISTER_OUTPUT_FOLDER = "device_register_2"
        val SERVER_ACC_REGISTER_OUTPUT_FOLDER = "server_accregister_2"
        val FIRST_CHARGE_OUTPUT_FOLDER = "first_charge_2"
        val DEVICE_FIRST_CHARGE_OUTPUT_FOLDER = "device_first_charge_2"
        val ROLE_REGISTER_OUTPUT_FOLDER = "roleregister_2"
        val LOGIN_LOGOUT_OUTPUT_FOLDER = "activity_2"
        val PAYMENT_OUTPUT_FOLDER = "payment_2"
        val MONEY_FLOW_OUTPUT_FOLDER = "money_flow_2"
        val CHARACTER_INFO_OUTPUT_FOLDER = "character_info_2"
        val TOTAL_ACC_LOGIN_OUTPUT_FOLDER = "total_login_acc_2"
        val TOTAL_DEVICE_LOGIN_OUTPUT_FOLDER = "total_login_device_2"
        val TOTAL_ACC_PAID_OUTPUT_FOLDER = "total_paid_acc_2"
        val TOTAL_DEVICE_PAID_OUTPUT_FOLDER = "total_paid_device_2"
        val CCU_OUTPUT_FOLDER = "ccu_2"
        val COUNTRY_MAPPING = "country_mapping_2"
    }
    
    object CCU {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val OS = "os"
        val CHANNEL = "channel"
        val CCU = "ccu"
    }

    object DEVICE_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val OS = "os"
        val DTY = "dty"
        val TEL = "tel"
        val NWK = "nwk"
        val S_HEIGHT = "sH"
        val S_WIDTH = "sW"
    }

    object COUNTRY_MAPPING_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
        val LOG_TYPE = "log_type"
        val COUNTRY_CODE = "country_code"
    }

    object LOGIN_LOGOUT_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val RID = "rid"
        val DID = "did"
        val ACTION = "action"
        val CHANNEL = "channel"
        val ONLINE_TIME = "online_time"
        val LEVEL = "level"
        val IP = "ip"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
    }

    object ACC_REGISTER_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val IP = "ip"
        val DEVICE = "device"
        val CHANNEL = "channel"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
    }
    object DEVICE_REGISTER_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val DID = "did"
        val ID = "id"
        val IP = "ip"
        val DEVICE = "device"
        val CHANNEL = "channel"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
    }

    object CHARACTER_INFO_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val LEVEL = "level"
        val RID = "rid"
        val ONLINE_TIME = "online_time"
        val LAST_LOGIN_DATE = "last_login_date"
        val REGISTER_DATE = "register_date"
        val FIRST_CHARGE_DATE = "first_charge_date"
        val LAST_CHARGE_DATE = "last_charge_date"
        val TOTAL_CHARGE = "total_charge"
        val MONEY_BALANCE = "money_balance"
        val FIRST_LOGIN_CHANNEL = "first_login_channel"
        val FIRST_CHARGE_CHANNEL = "first_charge_channel"
        val MORE_INFO = "more_info"
    }

    object SERVER_ACC_REGISTER_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
        val SID = "sid"
        val RID = "rid"
        val IP = "ip"
        val CHANNEL = "channel"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
        val DID = "did"
    }

    object TOTAL_ACC_LOGIN {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
    }
    object TOTAL_DEVICE_LOGIN {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val DID = "did"
        val ID = "id"
    }
    object TOTAL_DEVICE_PAID {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val DID = "did"
        val ID = "id"
    }

    object TOTAL_ACC_PAID {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
    }

    object ROLE_REGISTER_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
        val SID = "sid"
        val RID = "rid"
        val IP = "ip"
        val CHANNEL = "channel"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
        val DID = "did"
    }

    object PAYMENT_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val RID = "rid"
        val LEVEL = "level"
        val TRANS_ID = "trans_id"
        val CHANNEL = "channel"
        val PAY_CHANNEL = "pay_channel"
        val GROSS_AMT = "gross_amt"
        val NET_AMT = "net_amt"
        val XU_INSTOCK = "xu_instock"
        val XU_SPENT = "xu_spent"
        val XU_TOPUP = "xu_topup"
        val FIRST_CHARGE = "first_charge"
        val IP = "ip"
        val DEVICE = "device"
        val DID = "did"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
    }

    object MONEY_FLOW_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val RID = "rid"
        val LEVEL = "level"
        val TRANS_ID = "trans_id"
        val I_MONEY = "i_money"
        val MONEY_AFTER = "money_after"
        val MONEY_TYPE = "money_type"
        val REASON = "reason"
        val SUB_REASON = "sub_reason"
        val ADD_OR_REDUCE = "add_or_reduce"

    }

    object FIRST_CHARGE {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val ID = "id"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
    }

    object FIRSTCHARGE_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val IP = "ip"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
        val CHANNEL = "channel"
        val PAY_CHANNEL = "pay_channel"

    }
    object DEVICE_FIRSTCHARGE_FIELD {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val DID = "did"
        val ID = "id"
        val IP = "ip"
        val DEVICE = "device"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
        val CHANNEL = "channel"
        val PAY_CHANNEL = "pay_channel"

    }


    object PAYMENT_FIELD_V {
        val ADD = "0"
        val FROM_RECHARGE = "1"
        val GROSS_TO_NET = 200
    }
    //for contra only
    object CONTRA {
        val PAYMENT_ADD = "0"
        val PAYMENT_RECHARGE = "1"
    }
    //for KV only
    object KV {
        val RECHARGE_SUCCESS = "1"
        val DBG_ADD_PREFIX = "dbg"
    }

    object Kpi {

        // ACTIVE
        val ACTIVE = "active"
        val NEW_PLAYING = "new_playing"
        val NEW_ACCOUNT_PLAYING = "new_account_playing"
        val SERVER_NEW_ACCOUNT_PLAYING = "server_new_account_playing"
        val NEW_ROLE_PLAYING = "new_role_playing"
        val RETENTION_PLAYING = "retention_playing"
        val CHURN_PLAYING = "churn_playing"

        val NEW_USER_RETENTION = "new_user_retetion"
        val NEW_USER_RETENTION_RATE = "new_user_retetion_rate"
        val RETENTION_PLAYING_RATE = "retention_playing_rate"
        val RETENTION_PAYING_RATE = "retention_paying_rate"
        val CHURN_PLAYING_RATE = "churn_playing_rate"
        
        // REVENUE
        val PAYING_USER = "paying"
        val NET_REVENUE = "net_revenue"
        val GROSS_REVENUE = "gross_revenue"        // 2017-02-13 - vinhdp
        val RETENTION_PAYING = "retention_paying"
        val CHURN_PAYING = "churn_paying"
        val NEW_PAYING = "new_paying"
        val NEW_PAYING_NET_REVENUE = "new_paying_net_revenue"
        val NEW_PAYING_GROSS_REVENUE = "new_paying_gross_revenue"     // 2017-02-13 - vinhdp
        val NEW_USER_PAYING = "new_user_paying"
        val NEW_USER_PAYING_NET_REVENUE = "new_user_paying_net_revenue"
        val NEW_USER_PAYING_GROSS_REVENUE = "new_user_paying_gross_revenue"    // 2017-02-13 - vinhdp

        val ACU = "acu"
        val PCU = "pcu"

        val PLAYING_TIME = "playingtime"
        val AVG_PLAYING_TIME = "avg_playingtime"
        val CONVERSION_RATE = "conversion_rate"
        val ARRPU = "arrpu"
        val ARRPPU = "arrppu"
        
        // TRAFFIC
        val NRU = "nru"
        val NGR = "ngr"
        val CR = "cr"
        val RR = "rr"
        
        // USER RETENTION
        val USER_RETENTION = "user_retention"
    }

    object INPUT_FILE_TYPE {
        val TSV = "tsv"
        val PARQUET = "parquet"
        val HIVE = "hive"
        val JSON = "json"
    }

    object Timing {

        val A1 = "a1"
        val A7 = "a7"
        val A30 = "a30"
        val A60 = "a60"
        val A90 = "a90"
        val A180 = "a180"

        val AC1 = "ac1"
        val AC7 = "ac7"
        val AC30 = "ac30"
        val AC60 = "ac60"

        val A2 = "a2"
        val A3 = "a3"
        val A14 = "a14"
    }

    object ReportType {
        
        // Report By Game
        val GAME_USER = "game_user"
        val GAME_ACCOUNT_REGISTER = "game_accregister"
        val GAME_REVENUE = "game_revenue"
        val GAME_FIRST_CHARGE = "game_first_charge"
        val GAME_NEW_USER_PAYING = "game_new_user_paying"
        val GAME_NEW_USER_RETENTION = "game_new_user_retention"
        val GAME_USER_RETENTION = "game_user_retention"
        val GAME_CCU = "game_ccu"
        val GAME_FIRST_CHARGE_RETENTION = "game_first_charge_retention"
        
        // Report By Group
        val GROUP_USER = "group_user"
        val GROUP_ACCOUNT_REGISTER = "group_accregister"
        val GROUP_REVENUE = "group_revenue"
        val GROUP_FIRST_CHARGE = "group_first_charge"
        val GROUP_NEW_USER_PAYING = "group_new_user_paying"
        val GROUP_NEW_USER_RETENTION = "group_new_user_retention"
        val GROUP_USER_RETENTION = "group_user_retention"
        val GROUP_CCU = "group_ccu"
        val GROUP_FIRST_CHARGE_RETENTION = "group_first_charge_retention"
        
        // CALCULATING
        val CALC_ROLE_PLAYING_TIME = "calc_role_playing_time"
    }
    
    object ReportNumber {
        
        val CCU                     = "1"
        val ACCOUNT_REGISTER        = "2"
        val ACTIVE_USER             = "3"
        val USER_RETENTION          = "4"
        val NEWUSER_RETENTION       = "5"
        val NEWUSER_REVENUE         = "6"
        val REVENUE                 = "7"
        val FIRST_CHARGE            = "8"
        val FIRST_CHARGE_RETENTION  = "9"
        
        val ROLE_REGISTER           = "10"
        val SERVER_ACCOUNT_REGISTER = "11"
        
        val PLAYING_TIME            = "12"
        val GAME_RETENTION          = "13"
    }
    
    object GroupId {
        val GAME     = "game_code"
        val SID      = "sid"
        val CHANNEL  = "channel"
        val PACKAGE  = "package_name"
        val COUNTRY  = "country_code"
        val OS  = "os"
    }

    object CalcType {

        val DBG_ADD = "dbg_add"
        val WALLET_SPENT = "wallet_spent"
    }

    object Parameters {

        val JOB_NAME = "job_name"
        
        val RUN_TYPE = "run_type"
        val RUN_TIMING = "run_timing"
        
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val TIMING = "timing"
        val INPUT_PATH = "input_path"
        val OUTPUT_PATH = "output_path"
        val CALC_ID = "calc_id"
        val SOURCE = "source"
        val FILE_NAME = "file_name"
        val GROUP_ID = "group_id"

        val FROM_DATE = "from_date"
        val TO_DATE = "to_date"
        val RERUN_TIMING = "rerun_timing"
        val REPORT_TYPE = "report_type"
        val REPORT_NUMBER = "report_number"

        val ACTIVITY_PATH = "activity_path"
        val ACC_REGISTER_PATH = "acc_reg_path"
        val SERVER_ACC_REGISTER_PATH = "server_acc_reg_path"
        val ROLE_REGISTER_PATH = "role_reg_path"
        val PAYMENT_PATH = "payment_path"
        val FIRSTCHARGE_PATH = "firstcharge_path"
        val CCU_PATH = "ccu_path"
        val TOTAL_LOGIN_PATH = "total_login_path"
        val TOTAL_PAYING_PATH = "total_paying_path"
        
        val ROLE_PLAYING_TIME = "role_playing_time_path"
        val ACCOUNT_PLAYING_TIME = "account_playing_time_path"
        
        val MAPPING_COUNTRY = "mapping_country_path"
        val MAPPING = "mapping_path"
    }

    object ERROR_CODE {
        val PAYMENT_MOBILE_LOG_NULL = "pmmn"
        val PAYMENT_LOG_NULL = "pmn"
        val PAYMENT_LOG_APPEND = "pmap"
        val PAYMENT_LOG_DUPLICATE = "pmdup"

        val ACC_REGISTER_RESET = "accrr"
        val FIRST_CHARGE_RESET = "fcr"

        val DEVICE_REGISTER_RESET = "device-rr"
        val DEVICE_FIRST_CHARGE_RESET = "device-fcr"

        val ERROR = "error"
        val WARNING = "warning"
    }

    val MONEY_FLOW_ADD = 0
    val MONEY_FLOW_REDUCE = 1

}
