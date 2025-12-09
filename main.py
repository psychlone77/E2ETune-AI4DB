import os
import argparse
# from controller import tune
from config import parse_config
# from surrogate.train_surrogate import train_surrogate

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='your-ip', help='the database host')
    parser.add_argument('--database', type=str, default='tpch', help='workload file')
    parser.add_argument('--datapath', type=str, default='your/path', help='the database host')
    parser.add_argument('--prefix', type=str, default='job', help='workload file prefix')
    parser.add_argument('--workloadpath', type=str, default='your/path', help='workload file path')
    cmd = parser.parse_args()

    args = parse_config.parse_args("config/config.ini")

    args['ssh_config']['host'] = cmd.host
    args['database_config']['database'] = cmd.database
    args['database_config']['data_path'] = cmd.datapath
    args['tuning_config']['offline_sample'] += cmd.host
    print(args)

    all = os.listdir(cmd.workloadpath)
    workloads = [i for i in all if i.startswith(cmd.prefix)]

    print(workloads)
    if len(workloads) < 10:
        for workload in workloads:
            args['benchmark_config']['workload_path'] = cmd.workloadpath + workload
            try:
                # tune(workload, cmd.host, args)
                print(f'tuning {workload}...')
            except Exception as e:
                print(f'occur {e}')
                continue
    else:
        for idx in range(0, 13):
            workload = workloads[idx]
            args['benchmark_config']['workload_path'] = cmd.workloadpath + workload
            try:
                # tune(workload, cmd.host, args)
                print(f'tuning {workload}...')
            except Exception as e:
                print(f'occur {e}')
                continue
    
        # train_surrogate(cmd.database)
        print('surrogate model trained!')   

        for idx in range(0, len(workloads)):
            args['benchmark_config']['tool'] = 'surrogate'
            args['surrogate_config']['model_path'] = f'surrogate/{cmd.database}.pkl'
            args['surrogate_config']['feature_path'] = f'SuperWG/feature/{cmd.database}.json'
            args['benchmark_config']['workload_path'] = 'SuperWG/res/gpt_workloads/' + workloads[idx]
            try:
                # tune(workloads[idx], cmd.host, args)
                print(f'tuning {workloads[idx]} with surrogate...')
            except: continue

