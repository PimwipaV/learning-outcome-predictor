set -e
sudo -u ec2-user -i
source /home/ec2-user/anaconda3/bin/activate JupyterSystemEnv
pip install mysql-connector
pip install dynamodb-json
python3 -m pip install scikit-learn

cd /home/ec2-user/SageMaker/learning-outcome-predictor/

nohup python prep_job.py &