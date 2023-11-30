import boto3
import streamlit as st

def upload_csv(df, dataSource, null_flag, csv):

    dataSet = dataSource + "-dataset"
    csv_content = df.to_csv(index=False)
    bucket = "quicksight-automate"
    s3 = boto3.client('s3',
            aws_access_key_id="AKIA22HU66RI33ONQ4WH",
            aws_secret_access_key="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",
            region_name="ap-south-1"
            )

    print(csv)
    s3.put_object(Body=csv_content, Bucket=bucket, Key=csv)

    s3_manifest_content = '''
{
    "fileLocations": [
        {
            "URIs": [
                "s3://quicksight-automate/{csv}"
            ]
        }
    ],
    "globalUploadSettings": {
        "format": "CSV",
        "delimiter": ",",
        "textqualifier": "\\\"",
        "containsHeader": "true"
    }
}
    '''
    s3_manifest_content = s3_manifest_content.replace('{csv}', csv)
    file_name = "manifest.py"

    s3.put_object(Body=s3_manifest_content, Bucket=bucket, Key=file_name)


    print(f"Manifest file '{file_name}' uploaded to S3 bucket.")


    qs = boto3.client('quicksight',
            aws_access_key_id="AKIA22HU66RI33ONQ4WH",
            aws_secret_access_key="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",
            region_name="us-east-1"
            )

    response = qs.create_data_source(
        AwsAccountId='743543075921',
        Name=dataSource,
        DataSourceId=dataSource,
        Type='S3',
        DataSourceParameters={
            'S3Parameters': {
                'ManifestFileLocation': {
                    'Bucket': 'quicksight-automate',
                    'Key': 'manifest.py'
                },
                'RoleArn': 'arn:aws:iam::743543075921:role/service-role/aws-quicksight-service-role-v0'
            }
        },

        Permissions=[
            {
                'Principal': 'arn:aws:quicksight:us-east-1:743543075921:user/default/project',
                'Actions': [
                    'quicksight:DescribeDataSource',
                    'quicksight:DescribeDataSourcePermissions',
                    'quicksight:PassDataSource',
                    'quicksight:UpdateDataSource',
                    'quicksight:DeleteDataSource',
                    'quicksight:UpdateDataSourcePermissions'
                ]
            },
        ],
    )

    response_dataset = qs.create_data_set(
        AwsAccountId='743543075921',
        DataSetId=dataSet,
        Name=dataSet,
        PhysicalTableMap={
            'Data-Set-1': {
                'S3Source': {
                    'DataSourceArn': response['Arn'],
                    'InputColumns': [
                        {'Name': 'id', 'Type': 'STRING'},
                        {'Name': 'author', 'Type': 'STRING'},
                        {'Name': 'channel_url', 'Type': 'STRING'},
                        {'Name': 'title', 'Type': 'STRING'},
                        {'Name': 'webpage_url', 'Type': 'STRING'},
                        {'Name': 'view_count', 'Type': 'STRING'},
                        {'Name': 'like_count', 'Type': 'STRING'},
                        {'Name': 'duration', 'Type': 'STRING'},
                        {'Name': 'upload_date', 'Type': 'STRING'},
                        {'Name': 'tags', 'Type': 'STRING'},
                        {'Name': 'categories', 'Type': 'STRING'},
                        {'Name': 'description', 'Type': 'STRING'},
                        {'Name': 'thumbnail', 'Type': 'STRING'},
                        {'Name': 'best_format', 'Type': 'STRING'},
                        {'Name': 'filesize_bytes', 'Type': 'STRING'},
                    ],
                    'UploadSettings': {
                        'Format': 'CSV',
                        'StartFromRow': 1,
                        'ContainsHeader': True,
                    },
                }
            }
        },
        LogicalTableMap={
            'demo-logical': {
                'Alias': 'Test',
                'Source': {
                    'PhysicalTableId': 'Data-Set-1',
                },
                'DataTransforms': [
                    {
                        'CastColumnTypeOperation': {
                            'ColumnName': 'view_count',
                            'NewColumnType': 'INTEGER',
                        }
                    },
                    {
                        'CastColumnTypeOperation': {
                            'ColumnName': 'like_count',
                            'NewColumnType': 'INTEGER',
                        }
                    },
                    {
                        'CastColumnTypeOperation': {
                            'ColumnName': 'duration',
                            'NewColumnType': 'INTEGER',
                        }
                    },
                    {
                        'CastColumnTypeOperation': {
                            'ColumnName': 'upload_date',
                            'NewColumnType': 'INTEGER',
                        }
                    },
                    {
                        'CastColumnTypeOperation': {
                            'ColumnName': 'filesize_bytes',
                            'NewColumnType': 'INTEGER',
                        }
                    },
                ]
            }
        },
        ImportMode='SPICE',  
        Permissions=[
            {
                'Principal': 'arn:aws:quicksight:us-east-1:743543075921:user/default/project',
                'Actions': [
                    'quicksight:PassDataSet',
                    'quicksight:DescribeIngestion',
                    'quicksight:CreateIngestion',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:DescribeDataSet',
                    'quicksight:CancelIngestion',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:ListIngestions',
                    'quicksight:UpdateDataSetPermissions',
                ]
            },
        ],
    )

    print('Data Set Created')

    if null_flag:
        anal = '774e55f9-b82c-4c2c-b0e7-3c7cf9225031'
    else:
        anal = 'cbd4cb7e-a6dd-470c-95b0-3eb31b454b83'
    response_describe_analysis = qs.describe_analysis(
        AwsAccountId='743543075921',
        AnalysisId=anal
    )
    analysis_arn = response_describe_analysis['Analysis']['Arn']

    data_set_references = [
    {
        'DataSetPlaceholder': 'Data-Set-1',
        'DataSetArn': 'arn:aws:quicksight:us-east-1:743543075921:dataset/dssda-dataset',
    }
    ]

    response_template = qs.create_template(
        AwsAccountId='743543075921',
        TemplateId=dataSource + 'tem_id', 
        Name=dataSource + 'template', 
        Permissions=[
            {
                'Principal': 'arn:aws:quicksight:us-east-1:743543075921:user/default/project',
                'Actions': [
                    'quicksight:DescribeTemplate',
                    'quicksight:DescribeTemplatePermissions',
                    'quicksight:UpdateTemplate',
                    'quicksight:DeleteTemplate',
                    'quicksight:UpdateTemplatePermissions',
                ]
            },
        ],
            SourceEntity={
                'SourceAnalysis': {
                    'Arn': analysis_arn,
                    'DataSetReferences': data_set_references
                }
            }
        )
    st.write('Creating analysis...')
    
    response_analysis = qs.create_analysis(
        AwsAccountId='743543075921',
        AnalysisId=dataSource,
        Name=dataSource,
        SourceEntity={
            'SourceTemplate': {
                'DataSetReferences': [
                    {
                        'DataSetPlaceholder': 'Data-Set-1',
                        'DataSetArn': response_dataset['Arn']
                    },
                ],
                'Arn': response_template['Arn']
            }
        },
        Permissions=[
            {
                'Principal': 'arn:aws:quicksight:us-east-1:743543075921:group/SMASH',
                'Actions': [
                    'quicksight:DescribeAnalysis',
                    'quicksight:DescribeAnalysisPermissions',
                    'quicksight:UpdateAnalysis',
                    'quicksight:DeleteAnalysis',
                    'quicksight:QueryAnalysis',
                    'quicksight:UpdateAnalysisPermissions',
                    'quicksight:CreateTemplate',
                    'quicksight:CreateAnalysis',
                ]
            },
        ],
    )
    print(response_analysis)

    st.write("Analysis Created!")