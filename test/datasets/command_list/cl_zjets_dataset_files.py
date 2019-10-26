#!/bin/env python
base_command = '/net/gaugino/data1/jbkim/analysis/copydataset/modules/datasets/bin/copy_aods.py'
base_folder = './files/'
mid_folder = 'NanoAODv5/nano/2016/zjets'
print('# [global_key] dataset_files_info_filename : ./input_jsons/updated_mc_dataset_files_info.json')
print(base_command+' /store/mc/RunIIFall17NanoAODv5/ZJetsToNuNu_HT-100To200_13TeV-madgraph/NANOAODSIM/PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/40000/1137BB16-8497-4E41-B70F-4319BC3351C9.root '+base_folder+mid_folder)
