#!/bin/env python
base_command = '/net/gaugino/data1/jbkim/analysis/copydataset/modules/datasets/bin/copy_aods.py'
base_folder = './files/'
mid_folder = 'NanoAODv5/nano/2016/zjets'
print('# [global_key] dataset_files_info_filename : ./input_jsons/updated_mc_dataset_files_info.json')
print(base_command+' /store/mc/RunIISummer16NanoAODv5/ZJetsToNuNu_HT-100To200_13TeV-madgraph/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/120000/EF12DD95-023F-174C-A71F-E8ACD6E0D763.root '+base_folder+mid_folder)
