import os
from datetime import datetime

from tuner import run_tuning
from config import parse_config
from surrogate.train_surrogate import train_surrogate
import utils
import logging

if __name__ == '__main__':
    # Load configuration from config.ini
    args = parse_config.parse_args("config/config.ini")
    
    # Extract configuration values
    host = args['database_config']['host']
    database = args['database_config']['database']
    data_path = args['database_config']['data_path']
    benchmark = args['benchmark_config']['benchmark']
    workload_base_path = args['benchmark_config']['workload_path']
    
    # Setup main logger
    main_log_path = f"logs/tuning/main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = utils.get_logger(main_log_path, name='Main')
    print(logger)
    logger.info("="*100)
    logger.info("E2ETune: End-to-End Database Tuning System")
    logger.info("="*100)
    logger.info(f"Configuration file: config/config.ini")
    logger.info(f"Host: {host}")
    logger.info(f"Database: {database}")
    logger.info(f"Data Path: {data_path}")
    logger.info(f"Benchmark: {benchmark}")
    logger.info(f"Workload Path: {workload_base_path}")
    logger.info("="*100)

    # Discover workloads from configured path
    if not os.path.isdir(workload_base_path):
        logger.error(f"Workload path does not exist: {workload_base_path}")
        logger.error("Please check 'workload_path' in [benchmark_config] section of config.ini")
        exit(1)
    
    all_files = os.listdir(workload_base_path)
    workloads = [f for f in all_files if f.startswith(benchmark)]
    
    # Sort workloads in natural (numeric-aware) order, e.g. job_2.wg before job_10.wg
    workloads = utils.natural_sort(workloads)
    
    total_workloads = len(workloads)
    logger.info(f"Found {total_workloads} workloads matching prefix '{benchmark}'")

    # Check performance records to skip already-run workloads (resume support)
    perf_dir = args['benchmark_config'].get('performance_record_path', 'logs/performance_record')
    completed = utils.get_completed_workloads(perf_dir)
    if completed:
        logger.info(f"Found {len(completed)} completed workload records in: {perf_dir}")

    # Determine workload subset to tune (don't filter by completed yet)
    if total_workloads < 10:
        workloads_to_tune = workloads
        logger.info(f"Processing all {len(workloads)} workloads")
    else:
        workloads_to_tune = workloads[:13]
        logger.info(f"Processing first 13 of {len(workloads)} workloads")
    
    # Tune workloads
    successful = 0
    failed = 0
    skipped = 0
    
    for idx, workload in enumerate(workloads_to_tune, 1):
        # Check if this workload is already completed
        workload_id = os.path.splitext(workload)[0]
        if workload_id in completed or workload in completed or os.path.basename(workload) in completed:
            skipped += 1
            logger.info(f"[Workload {idx}/{len(workloads_to_tune)}] Skipping already completed: {workload}")
            continue
        
        args['benchmark_config']['workload_path'] = os.path.join(workload_base_path, workload)
        args['benchmark_config']['workload_name'] = workload
        
        try:
            logger.info("-" * 100)
            logger.info(f"[Workload {idx}/{len(workloads_to_tune)}] Starting tuning for: {workload}")
            logger.info("-" * 100)
            
            run_tuning(args, False)
            
            successful += 1
            logger.info(f"[Workload {idx}/{len(workloads_to_tune)}] Successfully completed tuning for: {workload}")
            
        except Exception as e:
            failed += 1
            logger.error(f"[Workload {idx}/{len(workloads_to_tune)}] Error tuning {workload}: {e}", exc_info=True)
            
            # For larger workload sets, stop on first failure
            if total_workloads >= 10:
                logger.error("Stopping due to error (large workload set)")
                break
            else:
                logger.warning("Continuing to next workload (small workload set)")
                continue
    
    # Summary
    logger.info("="*100)
    logger.info("TUNING SUMMARY")
    logger.info("="*100)
    logger.info(f"Total workloads processed: {len(workloads_to_tune)}")
    logger.info(f"Successfully tuned: {successful}")
    logger.info(f"Skipped (already completed): {skipped}")
    logger.info(f"Failed: {failed}")
    logger.info("="*100)
    logger.info("E2ETune session completed")
    logger.info("="*100)
    
    # Uncomment below for surrogate model training and usage
    # train_surrogate(database)
    # logger.info('Surrogate model trained!')   

    # for idx in range(0, len(workloads)):
    #     args['benchmark_config']['tool'] = 'surrogate'
    #     args['surrogate_config']['model_path'] = f'surrogate/{database}.pkl'
    #     args['surrogate_config']['feature_path'] = f'SuperWG/feature/{database}.json'
    #     args['benchmark_config']['workload_path'] = 'SuperWG/res/gpt_workloads/' + workloads[idx]
    #     try:
    #         run_tuning(args, True)
    #         logger.info(f'Tuning {workloads[idx]} with surrogate...')
    #     except Exception as e:
    #         logger.error(f'Error tuning {workloads[idx]} with surrogate: {e}')
    #         continue

