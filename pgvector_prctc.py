import textwrap
from typing import List
import random

import psycopg2

class VectorDB:
    _connection = None

    def __init__(self):
        pass

    def connect(self):
        connection = psycopg2.connect(
            host="",
            port=0000,
            database="",
            user="",
            password="",
        )

        cursor = connection.cursor()
        cursor.execute("SET TIMEZONE TO 'Asia/Seoul'")
        cursor.execute(f"SET search_path TO 'public'")
        cursor.close()

        self._connection = connection

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None


    def execute_query(self, query: str):
        self.execute_queries([query])

    def execute_queries(self, queries: List[str]):
        queries = [textwrap.dedent(query) for query in queries]

        connection = self.get_connection()
        for query in queries:
            if query.strip():
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
                cursor.close()
                
    def fetch_query(self, query: str):
        query = textwrap.dedent(query)

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()

        return result

    def explain_analyze_query(self, query: str):
        query = textwrap.dedent(f"EXPLAIN ANALYZE {query}")

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall() 
        cursor.close()

        return result

    def get_connection(self):
        self.connect()
        return self._connection
    




# 벡터 데이터 생성 함수
def generate_random_vector(dimension: int) -> str:
    vector = [random.uniform(-1, 1) for _ in range(dimension)]  # -1.0에서 1.0 사이의 랜덤 float 생성
    vector_str = ','.join(map(str, vector))  # 문자열로 변환하여 쉼표로 연결
    return f"'[{vector_str}]'"

# 10만개의 예시 데이터 삽입 함수
def insert_data(db: VectorDB, num_rows: int, dimension: int):
    queries = []
    for i in range(num_rows):
        name = f"Item {i + 1}"
        category_id = random.randint(1, 3)  # 1, 2, 3 중 랜덤 선택
        embedding = generate_random_vector(dimension)
        query = f"""
        INSERT INTO public.items (name, category_id, embedding)
        VALUES ('{name}', {category_id}, vector({embedding}));
        """
        queries.append(query)

        # 배치 실행: 한 번에 1000개의 쿼리를 실행
        if (i + 1) % 1000 == 0:
            db.execute_queries(queries)
            queries.clear()

    # 남은 쿼리 실행
    if queries:
        db.execute_queries(queries)

# 랜덤데이터 생성
"""
if __name__ == "__main__":
    vector_db = VectorDB()
    query = 
        CREATE TABLE public.items (
            id serial PRIMARY KEY,
            name TEXT,
            category_id INT,
            embedding vector(256)
        );
    vector_db.execute_query(query)
    try:
        dimension = 256  # 벡터의 차원
        num_rows = 1000  # 삽입할 데이터 수
        insert_data(vector_db, num_rows, dimension)
        print(f"Successfully inserted {num_rows} rows into the database.")
    finally:
        vector_db.close()
        """

embedding_vector = [
    0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.10, 
    0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20,
    0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.30,
    0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.40,
    0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.50,
    0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60,
    0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70,
    0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80,
    0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90,
    0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 0.100,
    0.101, 0.102, 0.103, 0.104, 0.105, 0.106, 0.107, 0.108, 0.109, 0.110,
    0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117, 0.118, 0.119, 0.120,
    0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128, 0.129, 0.130,
    0.131, 0.132, 0.133, 0.134, 0.135, 0.136, 0.137, 0.138, 0.139, 0.140,
    0.141, 0.142, 0.143, 0.144, 0.145, 0.146, 0.147, 0.148, 0.149, 0.150,
    0.151, 0.152, 0.153, 0.154, 0.155, 0.156, 0.157, 0.158, 0.159, 0.160,
    0.161, 0.162, 0.163, 0.164, 0.165, 0.166, 0.167, 0.168, 0.169, 0.170,
    0.171, 0.172, 0.173, 0.174, 0.175, 0.176, 0.177, 0.178, 0.179, 0.180,
    0.181, 0.182, 0.183, 0.184, 0.185, 0.186, 0.187, 0.188, 0.189, 0.190,
    0.191, 0.192, 0.193, 0.194, 0.195, 0.196, 0.197, 0.198, 0.199, 0.200,
    0.201, 0.202, 0.203, 0.204, 0.205, 0.206, 0.207, 0.208, 0.209, 0.210,
    0.211, 0.212, 0.213, 0.214, 0.215, 0.216, 0.217, 0.218, 0.219, 0.220,
    0.221, 0.222, 0.223, 0.224, 0.225, 0.226, 0.227, 0.228, 0.229, 0.230,
    0.231, 0.232, 0.233, 0.234, 0.235, 0.236, 0.237, 0.238, 0.239, 0.240,
    0.241, 0.242, 0.243, 0.244, 0.245, 0.246, 0.247, 0.248, 0.249, 0.250,
    0.251, 0.252, 0.253, 0.254, 0.255, 0.256
]

#데이터 검색
if __name__ == "__main__":
    query = f"SELECT * FROM items ORDER BY embedding <-> '{embedding_vector}' LIMIT 5;"
    vectorDB = VectorDB()
    vectorDB.connect()
    results = vectorDB.fetch_query(query)
    for result in results:
        print(result)
    vectorDB.close()
