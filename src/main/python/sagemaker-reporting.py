import json
import boto3
import datetime
import os
from aws_lambda_powertools import Logger

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOGGER = Logger(service="SagemakerReporting", level=LOG_LEVEL)

@LOGGER.inject_lambda_context
def handler(event, context):
    
    emailobject = {}
    db_client = boto3.client('dynamodb')
    dynamodb = boto3.resource('dynamodb')
    
    # Get DB Table Name from environment variable
    db_table_name = os.environ['SrvTableName']
    email_to = os.environ['MAILTO']
    email_cc = os.environ['MAILCC']
    emailobject['mailfrom'] = os.environ['MAILFROM']
    table = dynamodb.Table(db_table_name)
    
    # Get the AWS Account ID, Name & Region
    env_list = {'993990991017': 'TR-LABS-PREPROD', '451191978663': 'TR-LABS-PROD'}
    ACCOUNT_ID = context.invoked_function_arn.split(":")[4]
    region = context.invoked_function_arn.split(":")[3]    
    ENV = env_list[ACCOUNT_ID]

    _notebookWorkspaceID = []
    _notebookServiceOwner = []
    _notebookInstanceStatus = []
    _notebookInstanceName = []
    
    stoppedInstances = 0
    inserviceInstances = 0
    pendingInstances = 0
    stoppingInstances = 0
    failedInstances = 0
    deletingInstances = 0
    updatingInstances = 0

    db_res = table.scan(
            ScanFilter={
                'notebook_name': {
                    'ComparisonOperator': 'NOT_NULL'
                    },
                'service_owner': {
                    'ComparisonOperator': 'NOT_NULL'
                    }
            }
    )
    
    # Iterate through all instances and get status & ownership details
    # and send email to admin
    for x in db_res['Items']:
        # print(x)
        if x['status'] == "New" or x['status'] == "Failed" or x['status'] == "Error" or "emr" in x['service_name']:
            print("skipping failed notebooks and not sagemaker instances")
        else:
            notebookWorkspaceID = x['workspace_id']
            notebookServiceOwner = x['service_owner']
            notebookInstanceStatus = x['status']
            notebookInstanceName = x['notebook_name']
    
            # print ('workspace_id: ' + notebookWorkspaceID + ' notebookInstanceName: ' + notebookInstanceName + ' status: ' + notebookInstanceStatus + ' notebookServiceOwner: ' + notebookServiceOwner)
    
            if notebookInstanceStatus == "Stopped":
                stoppedInstances += 1
            elif notebookInstanceStatus == "Pending":
                pendingInstances += 1
            elif notebookInstanceStatus == "Started" or notebookInstanceStatus == "Starting":
                inserviceInstances += 1
                # y = inserviceInstances - 1
                _notebookWorkspaceID.append(x['workspace_id'])
                _notebookServiceOwner.append(x['service_owner'])
                _notebookInstanceStatus.append(x['status'])
                _notebookInstanceName.append(x['notebook_name'])
            elif notebookInstanceStatus == "Stopping":
                stoppingInstances += 1
            elif notebookInstanceStatus == "Deleting":
                deletingInstances += 1
            elif notebookInstanceStatus == "Updating":
                updatingInstances += 1
    
    ##############################################
    ### Prepare Email content for Daily Report ###
    ##############################################
    
    emailobject['subject'] = 'Daily Report: SCW Sagemaker in {env} | {reg}'.format(env=ENV, reg=region)
    message = """
                Hello C3 Admins,
                <br>
                <br> Below are all SCW Sagemaker Notebook Instances with their current Status. Respective Owners of In Service instances have been notified to shutdown if not in use.
                <br>
                <br>
                <table style="width:100%; border: 1px solid black; border-collapse: collapse;">
                    <caption><b>Daily Summary</b></caption>
                    <tr style="border: 1px solid black;">
                        <th style="border: 1px solid black;">Notebook Instance Status</th>
                        <th style="border: 1px solid black;">Number of Instances</th>
                    </tr>
                    <tr style="border: 1px solid black;">
                        <td style="border: 1px solid black;text-align: center;">In Service</td>
                        <td style="border: 1px solid black;text-align: center;">{inserviceInstances}</td>
                    </tr>
                    <tr style="border: 1px solid black;">
                        <td style="border: 1px solid black;text-align: center;">Stopped</td>
                        <td style="border: 1px solid black;text-align: center;">{stoppedInstances}</td>
                    </tr>
                </table>
                <br>
                <br>
                <table style="width:100%; border: 1px solid black; border-collapse: collapse;">
                    <caption><b>Running Instance Details</b></caption>
                    <tr style="border: 1px solid black;">
                        <th style="border: 1px solid black;">Notebook Instance Name</th>
                        <th style="border: 1px solid black;">Instance Status</th>
                        <th style="border: 1px solid black;">Instance Owner</th>
                    </tr>""".format(inserviceInstances=inserviceInstances,stoppedInstances=stoppedInstances)
                    # format(inserviceInstances=inserviceInstances,stoppedInstances=stoppedInstances,stoppingInstances=stoppingInstances,pendingInstances=pendingInstances,deletingInstances=deletingInstances,updatingInstances=updatingInstances)

    for x in range(0, inserviceInstances):
        message = message + """
                    <tr style="border: 1px solid black;">
                        <td style="border: 1px solid black;text-align: center;">{_notebookName}</td>
                        <td style="border: 1px solid black;text-align: center;">{_notebookStatus}</td>
                        <td style="border: 1px solid black;text-align: center;">{_notebookOwner}</td>
                    </tr>
                    """.format(_notebookName=_notebookInstanceName[x],_notebookStatus=_notebookInstanceStatus[x],_notebookOwner=_notebookServiceOwner[x])

    message = message + """
                </table>
                <br>
                <br> Please write to c3-scw-admin@thomsonreuters.com for assistance.
                <br><br>
                <b>What is this email about?</b>
                <br>
                This email is sent by C3 SCW - a platform service used for access to TR data and provisions cloud and security compliant compute environments for scientists to explore, visualize, and model Thomson Reuters' data.
                
                For more information, refer to the Secure Content Workspace Policy Guidance & FAQs.                            
                    """
    
    # Get the final message to be published as html
    emailobject['message'] = message
    
    # Get the To & CC Email addresses from environment variables
    emailobject['mailto'] = email_to
    emailobject['mailcc'] = email_cc
    
    # Send Email to Admins
    sendemail(emailobject)

    ##############################################
    ### Send Email to Owners  ####################
    ##############################################
    for owner in set(_notebookServiceOwner):
        name = owner.split("@")[0].split(".")[0].capitalize()
        messagebody = """
                    Dear {User},
                    <br>
                    <br> This is to inform you that your SCW Sagemaker notebook instance is running. Please do not forget to shutdown the <b>not in use</b> instances. It is advised to keep your instances in shutdown state when Not In Use!
                    <br>
                    <br>
                    <table style="width:100%; border: 1px solid black; border-collapse: collapse;">
                    """.format(User=name)
        for x in range(0, len(_notebookServiceOwner)):
            if owner == _notebookServiceOwner[x]:
                messagebody = messagebody + """
                            <tr style="border: 1px solid black;">
                                <td style="border: 1px solid black;text-align: center;">{_notebookName}</td>
                                <td style="border: 1px solid black;text-align: center;">{_notebookStatus}</td>
                                <td style="border: 1px solid black;text-align: center;">{_notebookOwner}</td>
                            </tr>
                            """.format(_notebookName=_notebookInstanceName[x],_notebookStatus=_notebookInstanceStatus[x],_notebookOwner=_notebookServiceOwner[x])

        messagebody = messagebody + """
                            </table>
                            <br>
                            <br> Please write to c3-scw-admin@thomsonreuters.com for assistance.
                            <br><br>
                            <b>What is this email about?</b>
                            <br>This is an advisory notification.
                            <br>
                            This email is sent by C3 SCW - a platform service used for access to TR data and provisions cloud and security compliant compute environments for scientists to explore, visualize, and model Thomson Reuters' data.
                            <br>
                            For more information, refer to the Secure Content Workspace Policy Guidance & FAQs.        
                            """
        
        # Get the final message to be published as html
        emailobject['message'] = messagebody
        emailobject['mailto'] = owner
        emailobject['mailcc'] = owner
        emailobject['subject'] = 'Alert: Your SCW Sagemaker Instance is Running! in {env} | {reg}'.format(env=ENV, reg=region)
        sendemail(emailobject)

    return 'Execution Completed.'
    
def sendemail(emailobject):
    mailto = emailobject['mailto']
    mailcc = emailobject['mailcc']
    mailfrom = emailobject['mailfrom']
    
    if mailcc == mailto:
        destination = {'ToAddresses': [mailto]}
    else:
        destination = {'ToAddresses': [mailto],'CcAddresses': [mailcc]}

    subject = emailobject['subject']
    message = emailobject['message']

    ses = boto3.client('ses')
    ses.send_email(
            Source=mailfrom,
            Destination=destination,
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'utf8'
                },
                'Body': {
                    'Html': {
                        'Data': message,
                        'Charset': 'utf8'
                    }
                }
            }
        )
    return 'INFO: Email sent'          