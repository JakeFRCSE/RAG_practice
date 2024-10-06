import psycopg2 as pg2

conn=pg2.connect(database="데이터베이스 이름",
                 host="데이터베이스 엔드포인트",
                 port="5432",
                 user="데이터베이스 유저네임",
                 password="데이터베이스 비밀번호")

cur = conn.cursor() 


# 테이블 만들때 실행
cur.execute("CREATE TABLE text_test1 (id serial PRIMARY KEY, text varchar);") 

# 테이블에 row 추가
cur.execute("INSERT INTO text_test1 (id, text) VALUES (%s, %s);", (2, "부산대 기계공학부도 어쩌고 저쩌고"))
            
cur.execute("SELECT * FROM text_test1") 
(id, text) = cur.fetchone() 
print(f"{id}, {text}")

#커밋 해야 저장됨
conn.commit()


cur.close()
conn.close()