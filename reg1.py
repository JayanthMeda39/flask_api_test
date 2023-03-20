from flask import Flask, request
from werkzeug.exceptions import HTTPException
# from boto3 import exceptions
import botocore.exceptions as be
from dynodb import Dynamodb as Ddb
import werkzeug.exceptions as we
import re

app = Flask(__name__)
table = Ddb('Userdb1')

'''def table_list():
    try:
        response = ddb.cli.list_tables()
        return response['TableNames']
    except be.ClientError as err:
        return str(err.response['Error'])


def del_table(table_name):
    try:
        response = ddb.cli.delete_table(TableName=table_name)
        return response
    except be.ClientError as err:
        return str(err.response['Error'])'''


@app.errorhandler(HTTPException)
def error_handler(error: HTTPException):
    response = {
        'error': error.__class__.__name__,
        'message': error.description
    }
    return response


@app.route('/registration', methods=['POST'])
def registration():
    try:
        data = request.get_json()
        email_regx = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        if ('email' not in data or 'password' not in data) and re.fullmatch(email_regx, data['email']):
            raise KeyError('Keys or missing/email format not matched')
        acc = table.query(['email', data['email']])
        if acc:
            return 'account already exists', 400
        else:
            response = table.put_item(data)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return 'Items Created in table', 201
            else:
                return 'Items Not created', 400
    except KeyError as err:
        return str(err)
    except we.BadRequest:
        return 'Bad request', 400
    except be.ClientError as err:
        return str(err.response['Error']), 400
    except AttributeError:
        return "Attribute Error", 400


@app.route('/show_data', methods=['GET'])
def show_data():
    try:
        email = request.args.get('email')
        pwd = request.args.get('password')
        if email is None or pwd is None:
            raise KeyError('Key is missing or misspelled')
        acc = table.get_item({'email': email, 'password': pwd})
        if acc:
            all_obj = table.scan()
            return all_obj
        else:
            return 'User Not Found', 400
    except KeyError as err:
        return str(err), 400
    except be.ClientError as e:
        return str(e.response['Error']), 400
    except we.BadRequest:
        return 'Bad request', 400


@app.route('/delete_user', methods=['DELETE'])
def delete_user():
    try:
        email = request.form.get('email')
        pwd = request.form.get('password')
        if email is None or pwd is None:
            raise KeyError('Key is missing or misspelled')
        acc = table.get_item({'email': email, 'password': pwd})
        if acc:
            res = table.delete_item({'email': email, 'password': pwd})
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return 'User deleted', 201
        else:
            return 'No account with this email/password'
    except KeyError as err:
        return str(err), 400
    except we.BadRequest:
        return 'Bad Request', 400
    except be.ParamValidationError:
        return 'Invalid type for parameter'


@app.route('/update_user', methods=['PUT'])
def update_user_phno():
    try:
        update_data = request.get_json()
        if update_data is None:
            raise KeyError('Key is missing or misspelled')
        acc = table.get_item(update_data['key_val'])
        if acc:
            res = table.update_item(update_data['key_val'], acc, update_data['value1'], 'SET ')
            return res
        else:
            return 'User Not Found'
    except KeyError as err:
        return str(err)
    except we.BadRequest:
        return 'Bad Request', 400
    except be.ClientError as err:
        return str(err.response['Error'])
