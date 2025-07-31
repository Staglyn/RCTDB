#基础配置模块
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import uuid
import os
from flask import send_file, abort
from datetime import datetime
#Flask 是构建 Web 应用的框架,CORS 配置允许不同域名的前端页面访问这个 API
app = Flask(__name__)
CORS(app)  # 允许跨域请求


#数据库连接模块
# 数据库配置（与init_db.py统一）
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'tumor_database',
    'charset': 'utf8mb4',
    'use_unicode': True
}

# 初始化数据库连接池 (pool) - 就像同时雇佣了 5 个管理员，当有多人需要查阅资料时，不用每次都重新找管理员，提高效率
db_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    **db_config
)
#get_db_connection () 函数-申请使用管理员的流程
def get_db_connection():
    try:
        return db_pool.get_connection()
    except Error as e:
        print(f"数据库连接错误: {e}")
        return None

#数据库表结构创建模块
# 创建数据库表（匹配Excel表头）
def create_tables():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # 1. 罕见肿瘤类型表（存储Excel基础信息）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rare_tumor_types (
            code VARCHAR(10) PRIMARY KEY COMMENT '肿瘤类型编码（如RT001）',
            name VARCHAR(100) NOT NULL COMMENT '肿瘤中文名称',
            tumor_name VARCHAR(200) NOT NULL COMMENT '肿瘤英文名称（Tumor Name）',
            `system` VARCHAR(100) COMMENT '所属系统（`system`）',
            tumor_nature VARCHAR(100) COMMENT '肿瘤性质（The Nature of the Tumor）',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_name (name, tumor_name) COMMENT '确保肿瘤名称唯一'
        ) ENGINE=InnoDB COMMENT='罕见肿瘤类型字典表';
        """)
        
        # 2. 数据集表（存储Excel详细数据）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id VARCHAR(36) PRIMARY KEY COMMENT '数据集唯一标识（UUID）',
            code VARCHAR(10) NOT NULL COMMENT '关联肿瘤类型编码',
            data_type VARCHAR(50) NOT NULL COMMENT '数据类型（Data Type）',
            sequencing_tech TEXT COMMENT '测序技术（Sequencing technologies）',
            sample_size INT UNSIGNED NOT NULL CHECK (sample_size >= 0) COMMENT '样本量（sample size）',
            series_accession VARCHAR(50) COMMENT '系列编号（Series Accession）',
            pmid TEXT COMMENT '文献ID（PMID，多值用空格分隔）',
            geo_link TEXT COMMENT 'GEO链接（geo 链接）',
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            description TEXT COMMENT '数据集描述',
            data_source VARCHAR(255) COMMENT '数据来源',
                        file_path TEXT COMMENT '压缩包存储路径', 
            file_size DECIMAL(10,2) COMMENT '文件总大小（GB）',
            is_public TINYINT(1) DEFAULT 0 COMMENT '是否公开（1-公开，0-私有）',
            -- 外键关联
            FOREIGN KEY (code) 
                REFERENCES rare_tumor_types(code)
                ON DELETE RESTRICT
                ON UPDATE CASCADE,
            -- 索引优化
            KEY idx_tumor_type (code),
            KEY idx_data_type (data_type),
            KEY idx_series (series_accession)
        ) ENGINE=InnoDB COMMENT='罕见肿瘤数据集表';
        """)
        
        connection.commit()
        print("数据库表创建/更新成功")
        
    except Error as e:
        print(f"创建表错误: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 确保上传目录存在
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER



# API路由模块      接收请求→处理数据→返回结果
# 获取所有数据集
@app.route('/api/datasets', methods=['GET'])#路由定义模块
#告诉系统：当有客户端访问/api/datasets这个地址，并且使用 GET 方法（就像浏览网页时的请求方式）时，就由下面的get_datasets函数来处理。
def get_datasets():#主处理函数定义
    try:
        connection = get_db_connection()#数据库连接模块，负责建立与数据库的连接，
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor(dictionary=True)#数据库查询模块
        #cursor（游标）可以比作一个 "服务员"，负责在程序和数据库之间传递信息。dictionary=True表示让服务员把结果整理成字典形式（方便按字段名访问）
        
        query = """
        SELECT d.*
        FROM datasets d
        """
        #query是 SQL 查询语句，相当于给服务员的 "指令清单"，这里的指令是 "从 datasets 表中查询所有字段的信息"。
        cursor.execute(query)
        datasets = cursor.fetchall()
        #cursor.execute(query)是让服务员执行这个指令，cursor.fetchall()则是让服务员把所有查询结果带回来。
       

         # 结果返回模块
         #把查询到的数据集信息转换成 JSON 格式（一种前后端通用的数据交换格式）并返回给客户端，相当于把做好的饭菜打包好递给顾客。
        return jsonify(datasets)
    
    except Error as e:#异常处理模块
        return jsonify({'error': str(e)}), 500
    #这部分代码就像一个 "应急预案"，如果在执行过程中出现任何错误（比如查询语句写错了），就会捕获这个错误，并把错误信息以 JSON 格式返回，同时附带 500 状态码
    finally:#资源释放模块
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            #finally块中的代码无论程序是否出现错误都会执行，确保数据库资源被正确释放。
            # 可以类比为：不管客人用餐过程是否顺利，离开时服务员都会收拾餐桌、关闭灯光，节约资源。这里就是关闭游标和数据库连接，避免资源浪费。

# 获取单个数据集
@app.route('/api/dataset/<code>', methods=['GET'])
def get_dataset(code):
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT d.*, t.`system`, t.tumor_nature
        FROM datasets d
        JOIN rare_tumor_types t ON d.code = t.code  # 关联肿瘤类型表，获取system和tumor_nature
        """
        cursor.execute(query, (code,))
        dataset = cursor.fetchone()
        
        if dataset:
            # if dataset['last_updated']:
            #     dataset['last_updated'] = dataset['last_updated'].isoformat()
            return jsonify(dataset)
        else:
            return jsonify({'error': 'Dataset not found'}), 404
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 创建数据集（支持文件上传）
@app.route('/api/dataset', methods=['POST'])#定义一个可供前端调用的 API 接口。
def create_dataset():
    try:
        # 获取表单数据，从前端提交的表单中提取各项信息。
        #每个request.form.get('xxx')就像在快递单上查找 "收件人"、"地址" 等特定条目。
        code = request.form.get('tumorType')
        data_type = request.form.get('dataType')  # 对应Excel的Data Type
        sequencing_tech = request.form.get('sequencingTech')  # 测序技术
        sample_size = request.form.get('sampleCount')
        series_accession = request.form.get('seriesAccession')  # 系列编号
        pmid = request.form.get('pmid')  # 文献ID
        geo_link = request.form.get('geoLink')  # GEO链接
        description = request.form.get('description', '')
        dataset_name = request.form.get('name', '')
        
        # 验证必填字段
        required_fields = [code, data_type, sample_size]
        if not all(required_fields):
            return jsonify({'error': '缺少必填字段（肿瘤类型、数据类型、样本量）'}), 400
        

        #文件上传处理模块
        files = request.files.getlist('files')
        file_size = 0.0
        for file in files:
            if file.filename:
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
                file.save(file_path)
                file_size += os.path.getsize(file_path) / (1024 **3)  # 转换为GB
        #数据库操作模块
        # 连接数据库
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor()
        # 生成唯一ID
        dataset_id = str(uuid.uuid4())
        tumor_name,system,tumor_nature = (1,2,3)
        
        # 插入数据
        query = """
INSERT INTO datasets 
(data_type, sequencing_tech, sample_size, 
 series_accession, pmid, geo_link, description, 
 tumor_name, code)  # 调整字段顺序，code可以留空或作为可选字段
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
        cursor.execute(query, (
    data_type, sequencing_tech, sample_size,
    series_accession, pmid, geo_link, description,
    request.form.get('tumor-name'),  # 直接使用输入的肿瘤名称
    str(uuid.uuid4())[:10]  # 生成临时编码或留空
))
        
        connection.commit()
        return jsonify({'id': dataset_id, 'message': '数据集创建成功'})
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': f'服务器错误: {str(e)}'}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 更新数据集
@app.route('/api/dataset/<id>', methods=['PUT'])
def update_dataset(id):
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor()
        
        # 构建更新语句（支持所有字段更新）
        fields = []
        values = []
        
        if 'code' in data:
            fields.append("code = %s")
            values.append(data['code'])
        if 'data_type' in data:
            fields.append("data_type = %s")
            values.append(data['data_type'])
        if 'sequencing_tech' in data:
            fields.append("sequencing_tech = %s")
            values.append(data['sequencing_tech'])
        if 'sample_size' in data:
            fields.append("sample_size = %s")
            values.append(data['sample_size'])
        if 'series_accession' in data:
            fields.append("series_accession = %s")
            values.append(data['series_accession'])
        if 'pmid' in data:
            fields.append("pmid = %s")
            values.append(data['pmid'])
        if 'geo_link' in data:
            fields.append("geo_link = %s")
            values.append(data['geo_link'])
        if 'description' in data:
            fields.append("description = %s")
            values.append(data['description'])
        
        if not fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        values.append(id)
        
        query = f"UPDATE datasets SET {', '.join(fields)} WHERE id = %s"
        cursor.execute(query, tuple(values))
        
        if cursor.rowcount == 0:
            return jsonify({'error': 'Dataset not found'}), 404
        
        connection.commit()
        return jsonify({'message': 'Dataset updated successfully'})
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 删除数据集
@app.route('/api/dataset/<id>', methods=['DELETE'])
def delete_dataset(id):
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor()
        
        query = "DELETE FROM datasets WHERE id = %s"
        cursor.execute(query, (id,))
        
        if cursor.rowcount == 0:
            return jsonify({'error': 'Dataset not found'}), 404
        
        connection.commit()
        return jsonify({'message': 'Dataset deleted successfully'})
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 肿瘤类型管理接口
@app.route('/api/rare_tumor_types', methods=['GET'])
def get_rare_tumor_types():
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor(dictionary=True)
        
        query = "SELECT * FROM rare_tumor_types ORDER BY code"
        cursor.execute(query)
        tumor_types = cursor.fetchall()
        
        return jsonify(tumor_types)
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/rare_tumor_type', methods=['POST'])
def add_rare_tumor_type():
    data = request.json
    if not data or 'code' not in data or 'name' not in data or 'tumor_name' not in data:
        return jsonify({'error': '缺少必要字段（code, name, tumor_name）'}), 400
    
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor()
        
        query = """
        INSERT INTO rare_tumor_types 
        (code, name, tumor_name, system, tumor_nature) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            data['code'], 
            data['name'], 
            data['tumor_name'],
            data.get('system', ''),
            data.get('tumor_nature', '')
        ))
        
        connection.commit()
        return jsonify({'message': '肿瘤类型添加成功'})
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/api/rare_tumor_type/<code>', methods=['DELETE'])
def delete_rare_tumor_type(code):
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor()
        
        query = "DELETE FROM rare_tumor_types WHERE code = %s"
        cursor.execute(query, (code,))
        
        if cursor.rowcount == 0:
            return jsonify({'error': '肿瘤类型不存在'}), 404
        
        connection.commit()
        return jsonify({'message': '肿瘤类型删除成功'})
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 统计信息接口
@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
            
        cursor = connection.cursor(dictionary=True)
        
        # 数据集总数
        cursor.execute("SELECT COUNT(*) AS total_datasets FROM datasets")
        stats = cursor.fetchone()
        
        # 肿瘤类型总数
        cursor.execute("SELECT COUNT(*) AS total_tumor_types FROM rare_tumor_types")
        stats.update(cursor.fetchone())
        
        # 总样本数
        cursor.execute("SELECT SUM(sample_size) AS total_samples FROM datasets")
        total_samples = cursor.fetchone()['total_samples']
        stats['total_samples'] = total_samples if total_samples else 0

        
        
        # 示例数据
        stats['total_users'] = 134
        stats['total_publications'] = 220
        
        return jsonify(stats)
    
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# 下载数据集

# 允许的根目录（限制文件只能从该目录下载，增强安全性）
ALLOWED_ROOT = "E:\\BaiduNetdiskDownload\\p_data"

@app.route('/api/dataset/<code>/download_from_db', methods=['GET'])
def download_from_db(code):
    try:
        # 1. 从数据库查询file_path
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': '数据库连接失败'}), 500
        
        cursor = connection.cursor(dictionary=True)
        query = "SELECT file_path FROM datasets WHERE code = %s"  # 通过code查询路径
        cursor.execute(query, (code,))
        dataset = cursor.fetchone()
        
        if not dataset or not dataset['file_path']:
            return jsonify({'error': '未找到文件路径记录'}), 404
        
        # 2. 验证文件路径合法性
        file_path = dataset['file_path']
        # 检查路径是否在允许的根目录下（防止路径遍历攻击）
        if not file_path.startswith(ALLOWED_ROOT):
            return jsonify({'error': '无权访问该文件'}), 403
        
        # 3. 验证文件是否存在
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            return jsonify({'error': '文件不存在或已被删除'}), 404
        
        # 4. 验证文件类型（仅允许压缩包）
        allowed_extensions = ('.zip', '.tar.gz', '.rar')
        if not file_path.lower().endswith(allowed_extensions):
            return jsonify({'error': '仅支持下载压缩包文件'}), 400
        
        # 5. 发送文件给用户下载
        return send_file(
            file_path,
            as_attachment=True,  # 强制浏览器下载
            download_name=os.path.basename(file_path)  # 下载时显示的文件名
        )
    
    except Error as e:
        return jsonify({'error': f'数据库错误：{str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'下载失败：{str(e)}'}), 500
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    create_tables()  # 确保表结构最新
    app.run(debug=True)