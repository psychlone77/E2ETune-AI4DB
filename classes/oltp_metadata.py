BENCHMARK_METADATA: dict[str, dict[str, dict]] = {
    #  TPC-C
    "tpcc": {
        "NewOrder": {
            "tables": [
                "warehouse",
                "district",
                "customer",
                "item",
                "stock",
                "order_line",
                "oorder",
                "new_order",
            ],
            "read": False,
            "predicates": 4,
            "operators": {"SELECT": 3, "INSERT": 3, "UPDATE": 2},
        },
        "Payment": {
            "tables": ["warehouse", "district", "customer", "history"],
            "read": False,
            "predicates": 3,
            "operators": {"SELECT": 2, "UPDATE": 2, "INSERT": 1},
        },
        "OrderStatus": {
            "tables": ["customer", "oorder", "order_line"],
            "read": True,
            "predicates": 2,
            "operators": {"SELECT": 3},
        },
        "Delivery": {
            "tables": ["new_order", "oorder", "order_line", "customer"],
            "read": False,
            "predicates": 2,
            "operators": {"SELECT": 2, "UPDATE": 2, "DELETE": 1},
        },
        "StockLevel": {
            "tables": ["district", "order_line", "stock"],
            "read": True,
            "predicates": 3,
            "operators": {"SELECT": 3},
        },
    },
    #  Smallbank
    "smallbank": {
        "Amalgamate": {
            "tables": ["accounts", "savings", "checking"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 2, "UPDATE": 2, "DELETE": 1},
        },
        "Balance": {
            "tables": ["accounts", "savings", "checking"],
            "read": True,
            "predicates": 1,
            "operators": {"SELECT": 3},
        },
        "DepositChecking": {
            "tables": ["accounts", "checking"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 1, "UPDATE": 1},
        },
        "SendPayment": {
            "tables": ["accounts", "checking"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 2, "UPDATE": 2},
        },
        "TransactSavings": {
            "tables": ["accounts", "savings"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 1, "UPDATE": 1},
        },
        "WriteCheck": {
            "tables": ["accounts", "savings", "checking"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 2, "UPDATE": 1},
        },
    },
    #  Twitter
    "twitter": {
        "GetTweet": {
            "tables": ["tweets"],
            "read": True,
            "predicates": 1,
            "operators": {"SELECT": 1},
        },
        "GetTweetsFromFollowing": {
            "tables": ["tweets", "follows"],
            "read": True,
            "predicates": 2,
            "operators": {"SELECT": 2},
        },
        "GetFollowers": {
            "tables": ["follows"],
            "read": True,
            "predicates": 1,
            "operators": {"SELECT": 1},
        },
        "GetUserTweets": {
            "tables": ["tweets"],
            "read": True,
            "predicates": 1,
            "operators": {"SELECT": 1},
        },
        "InsertTweet": {
            "tables": ["tweets", "added_tweets"],
            "read": False,
            "predicates": 0,
            "operators": {"INSERT": 2},
        },
    },
    #  Wikipedia
    "wikipedia": {
        "AddWatchList": {
            "tables": ["watchlist"],
            "read": False,
            "predicates": 1,
            "operators": {"INSERT": 1},
        },
        "RemoveWatchList": {
            "tables": ["watchlist"],
            "read": False,
            "predicates": 1,
            "operators": {"DELETE": 1},
        },
        "UpdatePage": {
            "tables": ["page", "revision", "text", "logging"],
            "read": False,
            "predicates": 2,
            "operators": {"SELECT": 2, "INSERT": 2, "UPDATE": 1},
        },
        "GetPageAnonymous": {
            "tables": ["page", "revision", "text"],
            "read": True,
            "predicates": 2,
            "operators": {"SELECT": 3},
        },
        "GetPageAuthenticated": {
            "tables": ["page", "revision", "text", "watchlist"],
            "read": True,
            "predicates": 3,
            "operators": {"SELECT": 4},
        },
    },
    #  YCSB ─
    "ycsb": {
        "ReadRecord": {
            "tables": ["usertable"],
            "read": True,
            "predicates": 1,
            "operators": {"SELECT": 1},
        },
        "InsertRecord": {
            "tables": ["usertable"],
            "read": False,
            "predicates": 0,
            "operators": {"INSERT": 1},
        },
        "ScanRecord": {
            "tables": ["usertable"],
            "read": True,
            "predicates": 2,
            "operators": {"SELECT": 1},
        },
        "UpdateRecord": {
            "tables": ["usertable"],
            "read": False,
            "predicates": 1,
            "operators": {"UPDATE": 1},
        },
        "DeleteRecord": {
            "tables": ["usertable"],
            "read": False,
            "predicates": 1,
            "operators": {"DELETE": 1},
        },
        "ReadModifyWriteRecord": {
            "tables": ["usertable"],
            "read": False,
            "predicates": 1,
            "operators": {"SELECT": 1, "UPDATE": 1},
        },
    },
}
