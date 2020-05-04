# coding: utf-8
from flask import Flask, render_template, session, request, redirect, url_for, jsonify
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import Required, Length, Regexp
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

app = Flask(__name__)
bootstrap = Bootstrap(app)
app.secret_key = 'sfjejj!djl04939rj#'


class UserNameForm(FlaskForm):
    user_name = StringField('name:',
                            validators=[Required(), Length(min=1, max=10, message='10文字以内で入力してください'),
                                        Regexp('^[a-zA-Z0-9]+$', message='英数字で入力してください')
                                        ])
    submit = SubmitField('Submit')


def get_dynamodb_table(table_name):
    return boto3.resource('dynamodb').Table(table_name)


def check_user(user_name):
    table = get_dynamodb_table('zozo-image-user')
    response = table.get_item(
        Key={
            'user_id': user_name
        }
    )
    if 'Item' in response:
        return True
    else:
        return False


def add_user(user_name):
    table = get_dynamodb_table('zozo-image-user')
    table.put_item(
        Item={
            'user_id': user_name
        }
    )


def get_user_timeline(user_name):
    table = get_dynamodb_table('zozo-image-user-timeline')
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(user_name),
        ScanIndexForward=False
    )
    return response['Items']

# resourceだけ返却
def get_user_favorite():
    return get_dynamodb_table('zozo-image-user-favorite')

# resourceだけ返却
def get_image_summary():
    return get_dynamodb_table('zozo-image-summary')

def set_display_content(timeline, favorite_res, summary_res):
    for item in timeline:
        # summaryを検索
        summary_hit = summary_res.get_item(
            Key={
                'id': item['image_id'][0:8],
                'image_id': item['image_id']
            }
        )
        # favoriteを検索
        favorite_hit = favorite_res.get_item(
            Key={
                'user_id': item['user_id'],
                'image_id': item['image_id']
            }
        )
        # summaryが存在していれば、その情報を追加
        if 'Item' in summary_hit:
            item['category'] = summary_hit['Item']['category']
            item['brand'] = summary_hit['Item']['brand']
            item['sex'] = summary_hit['Item']['sex']
        # favoriteが存在していれば、フラグを立てる
        if 'Item' in favorite_hit:
            item['flg'] = favorite_hit['Item']['flg'] # like(2) / dislike(1)
        else:
            item['flg'] = 0
    print timeline
    return timeline

@app.route('/')
def index():
    user_name = session.get('user_name')
    if user_name is None or not check_user(user_name):
        return redirect(url_for('login'))
    else:
        timeline = get_user_timeline(user_name)
        favorite_resource = get_user_favorite()
        summary_resource = get_image_summary()
        items = set_display_content(timeline, favorite_resource, summary_resource)
        return render_template('index.html', user_name=user_name, items=items)


@app.route('/like', methods=['POST'])
def like():
    user_name = session.get('user_name')
    if user_name is None or not check_user(user_name):
        return redirect(url_for('login'))
    if request.method == 'POST' and request.headers['Content-Type'] == 'application/json':
        table = get_dynamodb_table('zozo-image-user-favorite')
        response = table.update_item(
            Key={
                'user_id': user_name,
                'image_id': request.json
            },
            AttributeUpdates={
                'flg': {
                    'Action': 'PUT',
                    'Value': 2
                },
                'updatetime': {
                    'Action': 'PUT',
                    'Value': datetime.now().strftime("%Y%m%d%H%M%S%f")
                }
            }
        )
        return jsonify(response)
    else:
        return redirect(url_for('index'))


@app.route('/unlike', methods=['POST'])
def unlike():
    user_name = session.get('user_name')
    if user_name is None or not check_user(user_name):
        return redirect(url_for('login'))
    if request.method == 'POST' and request.headers['Content-Type'] == 'application/json':
        table = get_dynamodb_table('zozo-image-user-favorite')
        response = table.update_item(
            Key={
                'user_id': user_name,
                'image_id': request.json
            },
            AttributeUpdates={
                'flg': {
                    'Action': 'PUT',
                    'Value': 1
                },
                'updatetime': {
                    'Action': 'PUT',
                    'Value': datetime.now().strftime("%Y%m%d%H%M%S%f")
                }
            }
        )
        return jsonify(response)
    else:
        return redirect(url_for('index'))


@app.route('/login', methods=['POST', 'GET'])
def login():
    form = UserNameForm()
    if request.method == 'POST' and form.validate():
        user_name = form.user_name.data
        if check_user(user_name):
            session['user_name'] = user_name
            return redirect(url_for('index'))
    return render_template('login.html', form=form)


@app.route('/member', methods=['POST', 'GET'])
def member():
    form = UserNameForm()
    if request.method == 'POST' and form.validate():
        user_name = form.user_name.data
        if not check_user(user_name):
            add_user(user_name)
            session['user_name'] = user_name
            return redirect(url_for('index'))
    return render_template('member.html', form=form)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)

