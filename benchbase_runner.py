import os
import logging
import time
import json
import shutil
import re

class BenchBaseRunner:
    def __init__(self, args, logger=None):
        self.benchmark_config = args['benchmark_config']
        self.database_config = args['database_config']
        self.logger = logger or logging.getLogger(__name__)

    def load_database(self, workload_path):

        benchbase_jar_dir = self.benchmark_config.get('benchbase_jar_dir', '/home/benchbase/target/benchbase-postgres')
        benchmark_name = self.benchmark_config.get('benchmark', 'tpcc')

        benchbase_config, benchbase_dir = self.copy_config_to_benchbase(workload_path, benchmark_name)
        
        print(f'Loading data for benchmark: {benchmark_name}')
        load_command = (
            f'cd {benchbase_jar_dir} && '
            f'java -jar benchbase.jar '
            f'-b {benchmark_name.lower()} '
            f'-c config/postgres/{os.path.basename(workload_path)} '
            f'--create=true --load=true --execute=false'
        )
        
        load_state = os.system(load_command)
        if load_state != 0:
            print(f'Data loading failed with exit code: {load_state}')
            return 0.0
        
        print('Data loaded successfully, starting benchmark execution...')
        time.sleep(2)
    
    def run_benchmark(self, workload_path, log_file):
        # Run BenchBase benchmark and return throughput
        # Get benchmark name from config.ini
        benchmark_name = self.benchmark_config.get('benchmark', 'tpcc')
        
        # Extract workload name from file (e.g., "sample_tpcc_config.xml" -> "sample_tpcc_config")
        workload_name = os.path.splitext(os.path.basename(workload_path))[0]
        
        # Create results directory structure: stress_test_results/tpcc_results/sample_tpcc_config/
        results_base = "stress_test_results"
        workload_base_dir = os.path.join(results_base, f"{benchmark_name}_results")
        workload_results_dir = os.path.join(workload_base_dir, workload_name)
        os.makedirs(workload_results_dir, exist_ok=True)
        
        # Convert to absolute path to avoid permission issues
        workload_results_dir = os.path.abspath(workload_results_dir)
        
        # Copy config to BenchBase directory and get the new path
        benchbase_config, benchbase_dir = self.copy_config_to_benchbase(workload_path, benchmark_name)
        
        # Prepare variables for benchbase command
        timestamp = int(time.time())
        config_filename = os.path.basename(workload_path)
        log_file_path = os.path.join(workload_results_dir, f'{benchmark_name.lower()}_{timestamp}.log')
        
        # Navigate to benchbase directory and run the jar
        benchbase_jar_dir = os.path.join(benchbase_dir, 'benchbase-postgres')
        
        # Build the java command with all variables filled in
        command = (
            f'cd {benchbase_jar_dir} && '
            f'java -jar benchbase.jar '
            f'-b {benchmark_name.lower()} '
            f'-c config/postgres/{config_filename} '
            f'--execute=true '
            f'--directory={workload_results_dir} '
            f'> {log_file_path} 2>&1'
        )
        
        print(f'Running BenchBase with benchmark: {benchmark_name}')
        print(f'Config file: {config_filename}')
        print(f'Results will be saved to: {workload_results_dir}')
        print(f'Log file: {log_file_path}')
        
        state = os.system(command)
        
        # Cleanup - remove the copied config file
        # self.cleanup_config(benchbase_config)
        
        if state == 0:
            print('BenchBase running success')
        else:
            print(f'BenchBase running error - exit code: {state}')
            return 0.0

        # sleep for a while to ensure files are written
        time.sleep(5)
        
        # Clean up results and find summary.json
        summary_path = self.clean_and_find_summary(workload_results_dir)
        if not summary_path:
            print('No summary.json found in results')
            return 0.0
        
        # Parse throughput from summary.json
        throughput = self.parse_summary_json(summary_path)
        print(f'BenchBase {benchmark_name} throughput: {throughput}')
        
        return throughput
    
    def clean_and_find_summary(self, results_dir):
        """Find summary.json file, archive it in summary/ subdirectory, and delete everything else."""
        summary_path = None
        
        # Create summary subdirectory
        summary_archive_dir = os.path.join(results_dir, 'summary')
        os.makedirs(summary_archive_dir, exist_ok=True)
        
        # Wait a bit longer for file system to sync
        time.sleep(2)
        
        # Find .summary.json file
        for file in os.listdir(results_dir):
            # Skip the summary subdirectory
            if file == 'summary':
                continue
                
            file_path = os.path.join(results_dir, file)
            
            if file.endswith('.summary.json'):
                # Archive the original summary file with its timestamp name
                archived_path = os.path.join(summary_archive_dir, file)
                shutil.copy2(file_path, archived_path)
                print(f'Archived summary file: {archived_path}')
                
                # Also save as 'summary.json' in main directory
                final_summary_path = os.path.join(results_dir, 'summary.json')
                shutil.copy2(file_path, final_summary_path)  # Changed from move to copy
                summary_path = final_summary_path
                print(f'Saved summary file as: {final_summary_path}')
        
        # Now delete non-summary files (do this after finding summary)
        for file in os.listdir(results_dir):
            if file == 'summary' or file == 'summary.json':
                continue
                
            file_path = os.path.join(results_dir, file)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    print(f'Removed file: {file_path}')
                except Exception as e:
                    print(f'Could not remove {file_path}: {e}')
        
        return summary_path
    
    def parse_summary_json(self, summary_path):
        """Parse throughput from summary.json file."""
        try:
            with open(summary_path, 'r') as f:
                data = json.load(f)
            
            # Get throughput from metrics section
            throughput = data["Throughput (requests/second)"]
            
            print(f'Parsed throughput from summary.json: {throughput}')
            return throughput
            
        except Exception as e:
            print(f'Error parsing summary.json: {e}')
            return 0.0
    
    def update_config_file(self, config_file, benchmark_name):
        # Update BenchBase XML config with database settings using string replacement to preserve comments
        try:
            # Read the file as text
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Update database connection values
            host = self.database_config.get('host', 'localhost')
            port = self.database_config.get('port', '5432')
            database = self.database_config.get('database', 'benchbase')
            username = self.database_config.get('user', 'postgres')
            password = self.database_config.get('password', '')
            
            new_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=disable&amp;ApplicationName={benchmark_name}&amp;reWriteBatchedInserts=true"
            
            # Update URL
            content = re.sub(r'<url>.*?</url>', f'<url>{new_url}</url>', content)
            
            # Update username
            content = re.sub(r'<username>.*?</username>', f'<username>{username}</username>', content)
            
            # Update password
            content = re.sub(r'<password>.*?</password>', f'<password>{password}</password>', content)
            
            if database == 'ycsb':
                # update the scaling factor to 3600 for more challenging workload
                content = re.sub(r'<scalefactor>.*?</scalefactor>', '<scalefactor>3600</scalefactor>', content)
                # update rate to 15000
                content = re.sub(r'<rate>.*?</rate>', '<rate>70000</rate>', content)
            if database == 'wikipedia':
                content = re.sub(r'<rate>.*?</rate>', '<rate>unlimited</rate>', content)
                # set scale factor to 22
                content = re.sub(r'<scalefactor>.*?</scalefactor>', '<scalefactor>22</scalefactor>', content)
            if database == 'twitter':
                content = re.sub(r'<scalefactor>.*?</scalefactor>', '<scalefactor>80</scalefactor>', content)
                content = re.sub(r'<rate>.*?</rate>', '<rate>unlimited</rate>', content)
            if database == 'smallbank':
                content = re.sub(r'<scalefactor>.*?</scalefactor>', '<scalefactor>45</scalefactor>', content)
                content = re.sub(r'<rate>.*?</rate>', '<rate>unlimited</rate>', content)

            content = re.sub(r'<time>.*?</time>', '<time>60</time>', content)
            # Update terminals
            content = re.sub(r'<terminals>.*?</terminals>', '<terminals>16</terminals>', content)
            
            # Write back to file
            with open(config_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f'Updated config file: {config_file} (preserving comments)')
            
        except Exception as e:
            print(f'Error updating config file {config_file}: {e}')
    
    def copy_config_to_benchbase(self, workload_path, benchmark_name):
        # Update original config file first, then copy to BenchBase config directory
    
        # Update the original config file with database settings
        self.update_config_file(workload_path, benchmark_name)
        
        # Get BenchBase directory and config path
        benchbase_jar = self.benchmark_config.get('benchbase_jar', './benchbase/target/benchbase-postgres/benchbase.jar')
        benchbase_dir = os.path.dirname(os.path.dirname(benchbase_jar))  # Go up two levels from jar to benchbase root
        
        # Create the config directory path: target/benchbase-postgres/config/postgres
        config_dir = os.path.join(benchbase_dir, 'benchbase-postgres', 'config', 'postgres')
        os.makedirs(config_dir, exist_ok=True)
        
        # Copy with the actual filename
        config_filename = os.path.basename(workload_path)
        benchbase_config = os.path.join(config_dir, config_filename)
        
        shutil.copy2(workload_path, benchbase_config)
        print(f"Copied updated config to BenchBase config directory: {benchbase_config}")
        
        return benchbase_config, benchbase_dir
    
    def cleanup_config(self, config_file):
        # Remove the temporary config file
        try:
            os.remove(config_file)
            print(f"Cleaned up config file: {config_file}")
        except Exception as e:
            self.logger.warning(f"Could not cleanup config file {config_file}: {e}")
    
