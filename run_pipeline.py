#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Master ETL Pipeline
Chạy toàn bộ pipeline: Extract -> Load Staging -> Transform DW -> Load Mart
"""
import sys
import time
from datetime import datetime

# Import các ETL jobs
from etl_crypto_extract import ExtractCoinGecko
from etl_load_staging import LoadStaging
from etl_trans_load_dw import TransformDW
from etl_load_mart import LoadDataMart


class ETLPipeline:
    """Master ETL Pipeline orchestrator"""
    
    def __init__(self):
        self.jobs = [
            ("Extract", ExtractCoinGecko),
            ("Load Staging", LoadStaging),
            ("Transform DW", TransformDW),
            ("Load Data Mart", LoadDataMart)
        ]
        self.results = []
    
    def run_job(self, job_name: str, job_class):
        """Chạy một ETL job"""
        print(f"\n{'='*60}")
        print(f"Starting: {job_name}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            job = job_class()
            job.run()
            
            elapsed = time.time() - start_time
            self.results.append({
                "job": job_name,
                "status": "SUCCESS",
                "rows": job.row_count,
                "elapsed": elapsed
            })
            
            print(f"\n✓ {job_name} completed in {elapsed:.2f}s")
            return True
            
        except SystemExit as e:
            # Job đã xử lý lỗi và exit
            elapsed = time.time() - start_time
            self.results.append({
                "job": job_name,
                "status": "FAILED" if e.code != 0 else "SKIPPED",
                "rows": 0,
                "elapsed": elapsed
            })
            
            if e.code != 0:
                print(f"\n✗ {job_name} failed in {elapsed:.2f}s")
                return False
            else:
                print(f"\n⊘ {job_name} skipped in {elapsed:.2f}s")
                return True
        
        except Exception as e:
            elapsed = time.time() - start_time
            self.results.append({
                "job": job_name,
                "status": "ERROR",
                "rows": 0,
                "elapsed": elapsed
            })
            
            print(f"\n✗ {job_name} error in {elapsed:.2f}s: {e}")
            return False
    
    def print_summary(self):
        """In tóm tắt kết quả pipeline"""
        print(f"\n{'='*60}")
        print("PIPELINE SUMMARY")
        print(f"{'='*60}")
        
        total_elapsed = sum(r["elapsed"] for r in self.results)
        total_rows = sum(r["rows"] for r in self.results)
        
        for result in self.results:
            status_icon = {
                "SUCCESS": "✓",
                "FAILED": "✗",
                "SKIPPED": "⊘",
                "ERROR": "✗"
            }.get(result["status"], "?")
            
            print(f"{status_icon} {result['job']:<20} | "
                  f"{result['status']:<8} | "
                  f"{result['rows']:>8} rows | "
                  f"{result['elapsed']:>6.2f}s")
        
        print(f"{'-'*60}")
        print(f"Total: {total_rows} rows in {total_elapsed:.2f}s")
        
        # Kiểm tra có job nào failed không
        failed = [r for r in self.results if r["status"] in ["FAILED", "ERROR"]]
        if failed:
            print(f"\n⚠ {len(failed)} job(s) failed!")
            return False
        
        print(f"\n✓ All jobs completed successfully!")
        return True
    
    def run(self, stop_on_error: bool = True):
        """
        Chạy toàn bộ pipeline
        
        Args:
            stop_on_error: Dừng pipeline nếu có job lỗi (mặc định: True)
        """
        print(f"\n{'='*60}")
        print(f"ETL PIPELINE STARTED")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        overall_start = time.time()
        
        for job_name, job_class in self.jobs:
            success = self.run_job(job_name, job_class)
            
            if not success and stop_on_error:
                print(f"\n⚠ Pipeline stopped due to {job_name} failure")
                break
            
            # Delay nhỏ giữa các jobs
            if success:
                time.sleep(1)
        
        overall_elapsed = time.time() - overall_start
        
        # In summary
        success = self.print_summary()
        
        print(f"\nTotal pipeline time: {overall_elapsed:.2f}s")
        print(f"{'='*60}\n")
        
        return success


def main():
    """Entry point"""
    # Parse arguments
    stop_on_error = "--continue-on-error" not in sys.argv
    
    # Run pipeline
    pipeline = ETLPipeline()
    success = pipeline.run(stop_on_error=stop_on_error)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()