set -e
sudo -u ec2-user -i
source /home/ec2-user/anaconda3/bin/activate JupyterSystemEnv
pip install mysql-connector
pip install dynamodb-json
python3 -m pip install scikit-learn
cd /home/ec2-user/SageMaker/learning-outcome-predictor/

sudo mount -t nfs \
    -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 \
    172.28.71.186:/ \
    ../efs

sudo chmod go+rw ../efs
