import graphlab
env = graphlab.deploy.environment.EC2('pred-ec2', 's3://sample-testing/logs',region='us-east-1',instance_type='m3.xlarge',aws_access_key='AKIAICRIJDCGYA6JCAOQ',aws_secret_key='GsQUB5xSbndkfSwHwRXQiYwzqbav5GQtywnwViLb',num_hosts=3)
env.save()
