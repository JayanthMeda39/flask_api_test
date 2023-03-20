import boto3
from boto3 import exceptions
from boto3.dynamodb.conditions import Key, Attr


class Dynamodb:
    def __init__(self, table_name):
        try:
            self.table_name = table_name
            self.res = boto3.resource('dynamodb')
            self.table = self.res.Table(table_name)
        except exceptions.ResourceNotExistsError as e:
            print(e)

    def put_item(self, item_data):
        response = self.table.put_item(
            Item=item_data
        )
        return response

    def get_item(self, key, projexp=None):
        response = self.table.get_item(Key=key)
        if projexp:
            response = self.table.get_item(Key=key, ProjectionExpression=projexp)
            return response.get('Item')
        return response.get('Item')

    def query(self, key_conditions, filter_con=None):
        kwargs = {
            'KeyConditionExpression': Key(key_conditions[0]).eq(key_conditions[1])
        }
        if filter_con is not None:
            kwargs['FilterExpression'] = Attr(filter_con[0]).eq(filter_con[1])
        response = self.table.query(**kwargs)
        return response.get('Items')

    def scan(self):
        response = self.table.scan()
        return response.get('Items')

    def delete_item(self, key):
        response = self.table.delete_item(Key=key)
        return response

    # @staticmethod
    def find_by_key(self, data1, values):
        self.k = []
        for val in values:
            for key, value in data1.items():
                if isinstance(value, dict):
                    if val in value:
                        self.k.append([{val: values[val]}, key])
                elif key == val:
                    self.k.append({key: values[val]})
                    break
        return self.k

    def update_item(self, key_1, acc, update_data, ue):
        update_expression = ue
        expression_attribute_values = {}
        val_list = self.find_by_key(acc, update_data)
        for i in val_list:
            if len(i) > 1:
                for key, value in i[0].items():
                    update_expression += f'{i[1]}.{key} = :{key}, '
                    expression_attribute_values[f':{key}'] = value
            else:
                for key, value in i.items():
                    update_expression += f'{key} = :{key}, '
                    expression_attribute_values[f':{key}'] = value
        update_expression = update_expression[:-2]
        res = self.table.update_item(
            Key=key_1,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW'
        )
        return res.get('Attributes')
    '''def update_item(self, data, ue, map_name=None, add_list=None):
        update_expression = ue
        expression_attribute_values = {}
        expression_attribute_names = {f'#{map_name}': f'{map_name}'}
        data1 = data['values1']
        for attr_name, attr_value in data1.items():
            if attr_name in add_list:
                update_expression += f'#{map_name}.{attr_name} = :{attr_name}, '
            else:
                update_expression += f'{attr_name} = :{attr_name}, '
            expression_attribute_values[f':{attr_name}'] = attr_value
        update_expression = update_expression[:-2]
        print(update_expression)
        print(expression_attribute_values)
        print(data['key_val'])
        response = self.table.update_item(
            Key=data['key_val'],
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues='UPDATED_NEW'
        )
        return response.get('Attributes')'''

    '''def update_dynamo_item(self, item, primary_key_value, attribute_values):
        # Build the update expression and attribute values
        update_expression = 'SET '
        expression_attribute_values = {}
        for attribute_name, attribute_value in attribute_values.items():
            current_item = item
            attribute_path = attribute_name.split('.')
            for i, path in enumerate(attribute_path):
                if i == len(attribute_path) - 1:
                    if path in current_item:
                        update_expression += f'{attribute_name} = :val{i} '
                        expression_attribute_values[f':val{i}'] = attribute_value
                    else:
                        print(f"Attribute '{attribute_name}' not found in item")
                else:
                    if path in current_item:
                        current_item = current_item[path]
                    else:
                        print(f"Attribute path {'.'.join(attribute_path[:i + 1])} not found in item")
                        break

        # Update the item if there are any updates
        if len(expression_attribute_values) > 0:
            res = self.table.update_item(
                Key=primary_key_value,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='UPDATED_NEW'
            )
            return res.get('Attributes') '''
