endpoint_node = {
    "EndpointIdentifier": "database-1id",
    "EndpointType": "SOURCE",
    "EngineName": "sqlserver",
    "EngineDisplayName": "Microsoft SQL Server",
    "Username": "admin",
    "ServerName": "database-1.cs2rwctroxbf.eu-west-2.rds.amazonaws.com",
    "Port": 1433,
    "DatabaseName": "test_db",
    "Status": "active",
    "KmsKeyId": (
        "arn:aws:kms:eu-west-2:245260513500:key/a802067a-e288-441f-b63f-a7ecb5b783f4"
    ),
    "EndpointArn": "arn:aws:dms:eu-west-2:245260513500:endpoint:7DRJ5QSHOPBBCAYW76EA6SHZXMBCCV67XHPML4Y",
    "SslMode": "none",
    "MicrosoftSQLServerSettings": {
        "Port": 1433,
        "DatabaseName": "test_db",
        "ServerName": "database-1.cs2rwctroxbf.eu-west-2.rds.amazonaws.com",
        "Username": "admin",
    },
}

"""
            "schema-name": "test_schema",
            "table-name": "test_table"

"""

r = [
    {
        "rule-type": "selection",
        "rule-id": "814375997",
        "rule-name": "814375997",
        "object-locator": {"schema-name": "%", "table-name": "%"},
        "rule-action": "include",
        "filters": [],
    },
    {
        "rule-type": "selection",
        "rule-id": "856276228",
        "rule-name": "856276228",
        "object-locator": {"schema-name": "1%", "table-name": "2"},
        "rule-action": "exclude",
        "filters": [],
    },
]
# ggen = MssqlGenerator(host_settings='database-1.cs2rwctroxbf.eu-west-2.rds.amazonaws.com', databases='test_db')
# ee = EntitiesExtractor(rules_nodes=r,
#                        platform_host_url='http://localhost:8080', endpoint_engine=MssqlEngine(endpoint_node))
# #
# od_list = ee.get_oddrns_list()
