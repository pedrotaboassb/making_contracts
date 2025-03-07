from snowflake.snowpark import Session
import os
import logging
import re
from datetime import datetime
import ruamel.yaml
from collections import OrderedDict
yaml = ruamel.yaml.YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)


connection_parameters = {
"account": "cb20535.west-europe.azure",
"user": '',
"authenticator": "externalbrowser",
"role":  "DATA_ENGINEER_SAM",  # optional
"warehouse": "DEV_TRANSFORMING_SAM_WH",  # optional
}

contracts_dir = os.path.join(os.getcwd(), 'contracts')
print(contracts_dir)
print(os.listdir(contracts_dir))
yml_files = [os.path.splitext(file)[0] for file in os.listdir(contracts_dir) if file.endswith('.yml')]

def insert_after_name(each):
    new_dict = OrderedDict()
    for key, value in each.items():
        new_dict[key] = value
        if key == 'name':
            new_dict['config'] = {'contract': {'enforced': True}}
    return new_dict

print(yml_files)

with Session.builder.configs(connection_parameters).create() as session:
    logging.info("Connected to Snowflake")

    schema_name = 'EXTERNAL_EXPORT_SAM.AUXALITY'


    tables_df = session.sql(f"SHOW VIEWS IN SCHEMA {schema_name}").collect()

    data = [row.as_dict(True)['name'] for row in tables_df]
    print(data)


    for table_name in data:
        print(f"Querying table: {table_name}")
        for file in yml_files:
            if str(table_name).upper() == str(file).upper():
                print(str(table_name).upper(),str(file).upper())
                with open(f"./contracts/{file}.yml", 'r') as stream:
                    print(f"./contracts/{file}.yml")
                    try:

                        d = yaml.load(stream)
                        d['models'] = [dict(insert_after_name(each)) for each in d['models']]
                        counts_loop =1

                        for row in session.sql(f"desc table {schema_name}.{table_name}").collect():
                            for each in d['models']:


                                for i, columns in enumerate(each['columns']):
                                    counts_loop+=1
                                    if str(columns['name']).upper() == str(row.name).upper():
                                        new_columns = OrderedDict()
                                        for key, value in columns.items():
                                            new_columns[key] = value
                                            if key == 'name':
                                                if "VARCHAR" in row.type:
                                                    new_columns['data_type'] = re.match(r'^\w+', str(row.type)).group(0).lower()
                                                else:
                                                    new_columns['data_type'] = str(row.type).lower()
                                        columns =  dict(new_columns)
                                        each['columns'][i] = dict(new_columns)
                    except ruamel.yaml.YAMLError as e:
                        print(e)

                with open(f"./contracts/updated_{file}.yml", 'w') as f:
                    yaml.dump(d, f)
