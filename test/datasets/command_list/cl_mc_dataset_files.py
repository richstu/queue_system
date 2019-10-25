#!/bin/env python
base_command = '/net/gaugino/data1/jbkim/analysis/copydataset/modules/datasets/bin/copy_aods.py'
base_folder = './files/'
mid_folder = 'NanoAODv5/nano/2016/mc'
print('# [global_key] dataset_files_info_filename : ./input_jsons/mc_dataset_files_info.json')
print(base_command+' /store/mc/RunIISummer16NanoAODv5/TTJets_SingleLeptFromT_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/100000/73DD184E-16EC-A547-BDC5-C6E382EA6BB9.root '+base_folder+mid_folder)
