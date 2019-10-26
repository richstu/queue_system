#!/bin/env python
base_command = '/net/gaugino/data1/jbkim/analysis/copydataset/modules/datasets/bin/copy_aods.py'
base_folder = './files/'
mid_folder = 'NanoAODv5/nano/2016/mc'
print('# [global_key] dataset_files_info_filename : ./input_jsons/mc_dataset_files_info.json')
print(base_command+' /store/mc/RunIIFall17NanoAODv5/TTJets_HT-800to1200_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/250000/55AD773A-A139-764C-9F92-DD0D0FCE1A83.root '+base_folder+mid_folder)
print(base_command+' /store/mc/RunIIAutumn18NanoAODv5/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/40000/6300F12E-52CE-5147-88BC-617864E23B9E.root '+base_folder+mid_folder)
print(base_command+' /store/mc/RunIIAutumn18NanoAODv5/WJetsToLNu_HT-200To400_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/100000/C512AD82-A0EB-7741-9289-56C3387DB4E2.root '+base_folder+mid_folder)
