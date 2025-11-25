#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script kiểm tra và khởi tạo database
Tự động chạy setup_config_db.sql nếu chưa có schema control
"""
import os
import sys
import pymysql
from pathlib import Path


class DatabaseSetup:
    """Quản lý setup database"""
    
    def __init__(self):
        self.host = 'localhost'
        self.port = 3306
        self.user = 'root'
        self.password = ''
        self.db_name = 'dw'
        self.sql_file = 'setup_config_db.sql'
        
    def get_connection(self, use_db=True):
        """Tạo kết nối MySQL"""
        try:
            # Kết nối với database nếu use_db=True
            conn_params = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'password': self.password,
                'charset': 'utf8mb4',
                'autocommit': False
            }
            
            # Chỉ thêm database nếu use_db=True
            if use_db:
                conn_params['database'] = self.db_name
            
            conn = pymysql.connect(**conn_params)
            return conn
        except Exception as e:
            print(f"✗ Không thể kết nối MySQL: {e}")
            sys.exit(1)
    
    def check_schema_exists(self, conn) -> bool:
        """Kiểm tra schema control đã tồn tại chưa"""
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT SCHEMA_NAME 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME = 'control'
                """)
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            print(f"⚠ Lỗi kiểm tra schema: {e}")
            return False
    
    def check_config_table_exists(self, conn) -> bool:
        """Kiểm tra bảng config đã tồn tại chưa"""
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'control' 
                    AND TABLE_NAME = 'config'
                """)
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            print(f"⚠ Lỗi kiểm tra bảng config: {e}")
            return False
    
    def check_config_has_data(self, conn) -> bool:
        """Kiểm tra bảng config có dữ liệu chưa"""
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM `control`.`config`")
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            print(f"⚠ Lỗi kiểm tra dữ liệu: {e}")
            return False
    
    def run_sql_file(self, conn):
        """Đọc và chạy file SQL"""
        sql_path = Path(self.sql_file)
        
        if not sql_path.exists():
            print(f"✗ Không tìm thấy file: {self.sql_file}")
            sys.exit(1)
        
        print(f"\n📄 Đọc file: {self.sql_file}")
        
        try:
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Tách các câu lệnh SQL (bỏ qua comment blocks)
            lines = sql_content.split('\n')
            statements = []
            current_statement = []
            
            for line in lines:
                # Bỏ qua comment lines
                stripped = line.strip()
                if stripped.startswith('--') or not stripped:
                    continue
                
                current_statement.append(line)
                
                # Nếu gặp dấu ;, tạo statement
                if ';' in line:
                    stmt = '\n'.join(current_statement)
                    if stmt.strip():
                        statements.append(stmt)
                    current_statement = []
            
            print(f"⚙ Thực thi {len(statements)} câu lệnh SQL...")
            
            with conn.cursor() as cursor:
                for i, statement in enumerate(statements, 1):
                    try:
                        # Ensure we're using the dw database
                        cursor.execute(f"USE `{self.db_name}`")
                        cursor.execute(statement)
                        print(f"  ✓ Statement {i}/{len(statements)}")
                    except Exception as e:
                        print(f"  ⚠ Statement {i}: {str(e)[:100]}")
                
                conn.commit()
            
            print(f"✓ Hoàn thành chạy file SQL")
            return True
            
        except Exception as e:
            print(f"✗ Lỗi đọc/chạy file SQL: {e}")
            conn.rollback()
            return False
    
    def verify_setup(self, conn) -> bool:
        """Kiểm tra setup thành công"""
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM `control`.`config`")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    print(f"\n✓ Verify: Đã có {count} config records")
                    return True
                else:
                    print(f"\n✗ Verify: Bảng config rỗng")
                    return False
        except Exception as e:
            print(f"\n✗ Verify failed: {e}")
            return False
    
    def run(self):
        """Chạy setup database"""
        print("="*60)
        print("DATABASE SETUP")
        print("="*60)
        
        # Kết nối với database dw
        conn = self.get_connection(use_db=True)
        
        try:
            # 1. Kiểm tra schema control
            print("\nKiểm tra schema 'control'...")
            schema_exists = self.check_schema_exists(conn)
            
            if schema_exists:
                print("  ✓ Schema 'control' đã tồn tại")
                
                # 2. Kiểm tra bảng config
                print("\n Kiểm tra bảng 'config'...")
                table_exists = self.check_config_table_exists(conn)
                
                if table_exists:
                    print("  ✓ Bảng 'config' đã tồn tại")
                    
                    # 3. Kiểm tra dữ liệu
                    print("\n Kiểm tra dữ liệu config...")
                    has_data = self.check_config_has_data(conn)
                    
                    if has_data:
                        print("  ✓ Bảng 'config' đã có dữ liệu")
                        print("\n✅ Database đã được setup đầy đủ")
                        return True
                    else:
                        print("  ⚠ Bảng 'config' chưa có dữ liệu")
                        print("\n⚙ Chạy setup_config_db.sql để insert dữ liệu...")
                else:
                    print("  ⚠ Bảng 'config' chưa tồn tại")
                    print("\n⚙ Chạy setup_config_db.sql...")
            else:
                print("  ⚠ Schema 'control' chưa tồn tại")
                print("\n⚙ Chạy setup_config_db.sql...")
            
            # Chạy file SQL
            success = self.run_sql_file(conn)
            
            if success:
                # Verify
                if self.verify_setup(conn):
                    print("\n✅ Database setup hoàn tất!")
                    return True
                else:
                    print("\n⚠ Setup có vấn đề, vui lòng kiểm tra lại")
                    return False
            else:
                print("\n✗ Setup thất bại")
                return False
                
        except Exception as e:
            print(f"\n✗ Lỗi: {e}")
            return False
        finally:
            conn.close()


def main():
    """Entry point"""
    setup = DatabaseSetup()
    success = setup.run()
    
    print("\n" + "="*60)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()