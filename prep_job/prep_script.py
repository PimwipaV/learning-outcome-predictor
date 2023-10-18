from datetime import datetime, timedelta

yesterday = datetime.today() - timedelta(days=1) 

def get_date(yesterday):
# today I get the full day data of yesterday midnight Thai time = 17hrs UTC
#datetime.today()-timedelta(days=6)).strftime('%Y%m%d'

    #if not taking argument, have to assign this line
    #yesterday = datetime.today() - timedelta(days=1)

    gobackaday = yesterday-timedelta(days=1)
    gobackaday = gobackaday.strftime('%Y%m%d')
    yesterday = yesterday.strftime('%Y%m%d')
    start = gobackaday+"T17"
    end = yesterday+"T16"
    output_path = yesterday+".json"
    
    return start, end, output_path

start, end, output_path = get_date(yesterday)

    
import subprocess
from os import environ

#def execute_bash_script(script_path, argument, arg2, arg3):
    #subprocess.run(["bash", script_path, argument, arg2, arg3], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
def run_bash_script(script_file, *args):
    environ['AMP_KEY']='0c963c9c706603e502d5b415b1399da9'
    environ['AMP_SECRET']='7c1e57583cf24de7df6e9efb05208228'
    bash_command = [script_file]
    bash_command.extend(args)
    print(script_file)
    process = subprocess.Popen(bash_command, stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    return output, error

#execute_bash_script("./amp_export.sh", start, end, output_path)

