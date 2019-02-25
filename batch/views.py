import json
import uuid
import manage
from django.http import JsonResponse, HttpResponse
import batch.mock_executor.mock_executor as mock_executor
from django.core import serializers


job_definitions = []


# Create your views here.
def upload_s3(request, root_id, job_name, file_name):
    if request.method == 'PUT':
        return JsonResponse({})
    elif request.method == 'GET':
        return download_s3(job_name)


def download_s3_nf(request, root_dir, job_name, job_id, file_name):
    if file_name == ".exitcode":
        return JsonResponse(0)

    pass


def download_s3(request, root_id, job_name, job_id, file_name):
    if job_name == "root.bwa_mem":
        return JsonResponse({
          "output_sam" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sam",
            "checksum" : "sha1$4bdb0ace02d776bdedf2c6c08b5d0ae16e25df04",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.bwa_mem/b918caf4-6bc7-4466-9070-b30b01dfdf21/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.bwa_mem/b918caf4-6bc7-4466-9070-b30b01dfdf21/root/Sample_BRCA-00236-T_IGO_06208_2.sam",
            "metadata" : None,
            "nameext" : ".sam",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.bwa_mem/b918caf4-6bc7-4466-9070-b30b01dfdf21/root/Sample_BRCA-00236-T_IGO_06208_2.sam",
            "secondaryFiles" : [ ],
            "size" : 39299536701
          }
        }, content_type="application/json")
    elif job_name == "root.make_bam":
        return JsonResponse({
          "output_bam" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.bam",
            "checksum" : "sha1$4f59ba71078f6b341d68744be07800d9108de8ea",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.make_bam/99c8f671-eed9-44ec-97e8-ee81ab34af19/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.make_bam/99c8f671-eed9-44ec-97e8-ee81ab34af19/root/Sample_BRCA-00236-T_IGO_06208_2.bam",
            "metadata" : None,
            "nameext" : ".bam",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.make_bam/99c8f671-eed9-44ec-97e8-ee81ab34af19/root/Sample_BRCA-00236-T_IGO_06208_2.bam",
            "secondaryFiles" : [ ],
            "size" : 9907278455
          }
        }, content_type="application/json")
    elif job_name == 'root.sort_bam':
        return JsonResponse({
          "output_file" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.bam",
            "checksum" : "sha1$7b08450ea29a215a0aac63a0f51cb9f622a14c92",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.sort_bam/3c694f37-2ab2-4483-a4e6-5ebd2cef2166/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.sort_bam/3c694f37-2ab2-4483-a4e6-5ebd2cef2166/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.bam",
            "metadata" : None,
            "nameext" : ".bam",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.sort_bam/3c694f37-2ab2-4483-a4e6-5ebd2cef2166/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.bam",
            "secondaryFiles" : [ ],
            "size" : 6884091106
          }
        }, content_type="application/json")
    elif job_name == 'root.mark_duplicates':
        return JsonResponse({
          "output_md_bam" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bam",
            "checksum" : "sha1$f7db549d03373ac1a8583a2749d1589d91cd1f1a",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bam",
            "metadata" : None,
            "nameext" : ".bam",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bam",
            "secondaryFiles" : [ {
              "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bai",
              "checksum" : "sha1$497afe1e29bc5cfff8ec8e4a1ab0a6177e64a8ce",
              "class" : "File",
              "contents" : None,
              "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root",
              "format" : None,
              "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bai",
              "metadata" : None,
              "nameext" : ".bai",
              "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md",
              "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bai",
              "secondaryFiles" : [ ],
              "size" : 7219472
            } ],
            "size" : 9628229636
          },
          "output_md_metrics" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.txt",
            "checksum" : "sha1$d23400e9dab900f5626f6dfc7e6e1858d3eb57f1",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.txt",
            "metadata" : None,
            "nameext" : ".txt",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.mark_duplicates/d3f07423-b0e3-486c-88bd-7ba916fd089c/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.txt",
            "secondaryFiles" : [ ],
            "size" : 3885
          }
        }, content_type="application/json")
    elif job_name == 'root.base_recal':
        return JsonResponse({
          "output_tabl" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.tabl",
            "checksum" : "sha1$c2d725781d1c0623058f6940aa0279f17e0ce813",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.base_recal/e741706e-7e7d-409d-89d5-460f3af63ade/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.base_recal/e741706e-7e7d-409d-89d5-460f3af63ade/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.tabl",
            "metadata" : None,
            "nameext" : ".tabl",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.base_recal/e741706e-7e7d-409d-89d5-460f3af63ade/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.tabl",
            "secondaryFiles" : [ ],
            "size" : 526706
          }
        }, content_type="application/json")
    elif job_name == 'root.apply_bqsr':
        return JsonResponse({
          "output_bqsr" : {
            "basename" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bqsr.bam",
            "checksum" : "sha1$50217081349b3828559820372cd968312276a708",
            "class" : "File",
            "contents" : None,
            "dirname" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.apply_bqsr/edfba454-77c6-4112-9656-dfa910dba13f/root",
            "format" : None,
            "location" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.apply_bqsr/edfba454-77c6-4112-9656-dfa910dba13f/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bqsr.bam",
            "metadata" : None,
            "nameext" : ".bam",
            "nameroot" : "Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bqsr",
            "path" : "s3://cromwell-wf-results/jobs/fe94c095-5d78-4520-b089-1dab69ae5c1f/root.apply_bqsr/edfba454-77c6-4112-9656-dfa910dba13f/root/Sample_BRCA-00236-T_IGO_06208_2.sorted.md.bqsr.bam",
            "secondaryFiles" : [ ],
            "size" : 12677746076
          }
        }, content_type="application/json")


def submit_job(request):
    body = json.loads(request.body)
    job_id = "/".join(body['containerOverrides']["command"][4].split("s3://cromwell-wf-results/test/")[1].split('/')[0:2])
    result = {
        "jobId": job_id,
        "jobName": body['jobName']
    }
    manage.job_service.start_job(job_id, body['jobName'].split('_')[0])
    return JsonResponse(result)


def register_job_definition(request):
    body = json.loads(request.body)
    job_name = body['jobDefinitionName']
    name = body['containerProperties']['command'][8].split('/')[-1]
    return JsonResponse({})


def register_job_definition_nf(request):
    body = json.loads(request.body)
    job_name = body['jobDefinitionName']
    res = {
        "jobDefinitionArn": job_name,
        "jobDefinitionName": job_name,
        "revision": 1
    }
    job_definitions.append(body)
    return JsonResponse(res)


def describe_jobs(request):
    body = json.loads(request.body)
    result = {'jobs': [] }
    for i in body['jobs']:
        for job in mock_executor.completed:
            if i == job.job_name:
                completed = True
                result['jobs'].append({'status': 'SUCCEEDED', 'jobId': i})
                break
        if not completed:
            result['jobs'].append({'status': 'RUNNING', 'jobId': i})

    return JsonResponse(result)


def describe_job_definition(request):
    body = json.loads(request.body)
    job_name = body['jobDefinitionName']
    response = {
                    "jobDefinitions": [
                          {
                             "containerProperties": {

                                 "jobDefinitionArn": job_name,
                                 "jobDefinitionName": job_name,

                             }
                          }
                       ],
                       "nextToken": "string"
                    }
    # manage.job_service.start_job(job_name, name)
    return JsonResponse(response)


def list_jobs(request):
    result = []
    body = json.loads(request.body)
    if body['jobStatus'] == 'SUCCEEDED':
        for completed in mock_executor.completed:
            result.append(completed.dump())
        return JsonResponse({"jobSummaryList": result}, content_type="application/json")
    else:
        return JsonResponse({"jobSummaryList": []}, content_type="application/json")


def terminate_job(request):
    return JsonResponse({}, content_type="application/json")


def return_200(request):
    return JsonResponse({}, content_type="application/json")



result_S10_L004_R1_001 = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>wes_samples/Sample_BRCA-00067-N_IGO_06208_30/BRCA-00067-N_IGO_06208_30_S10_L004_R1_001.fastq.gz</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>wes_samples/Sample_BRCA-00067-N_IGO_06208_30/BRCA-00067-N_IGO_06208_30_S10_L004_R1_001.fastq.gz</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

result_S10_L004_R2_001 = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>wes_samples/Sample_BRCA-00067-N_IGO_06208_30/BRCA-00067-N_IGO_06208_30_S10_L004_R2_001.fastq.gz</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>wes_samples/Sample_BRCA-00067-N_IGO_06208_30/BRCA-00067-N_IGO_06208_30_S10_L004_R2_001.fastq.gz</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

result_S25_L006_R1_001 = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>wes_samples/Sample_BRCA-00067-T_IGO_06208_1/BRCA-00067-T_IGO_06208_1_S25_L006_R1_001.fastq.gz</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>wes_samples/Sample_BRCA-00067-T_IGO_06208_1/BRCA-00067-T_IGO_06208_1_S25_L006_R1_001.fastq.gz</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

result_S25_L006_R2_001 = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>wes_samples/Sample_BRCA-00067-T_IGO_06208_1/BRCA-00067-T_IGO_06208_1_S25_L006_R2_001.fastq.gz</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>wes_samples/Sample_BRCA-00067-T_IGO_06208_1/BRCA-00067-T_IGO_06208_1_S25_L006_R2_001.fastq.gz</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

result_b37_fasta = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>References/grch37/b37.fasta</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>References/grch37/b37.fasta</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

exist_result ="""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucketcmo</Name><Prefix>%s</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><LastModified>2019-01-18T18:54:46.000Z</LastModified><ETag>&quot;d68ab95e2be65dcbbfe22039c6945aa9-294&quot;</ETag><Size>2465271488</Size><Owner><ID>e49a0eeeeb5b63c45f828801983863637f607402e07c7c899e418ddbdcfcdea8</ID><DisplayName>aws_CMO_collab</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""

empty = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>cromwell-wf-results</Name><Prefix>test/37/cbd61b4b52e5b489ad370eb9946699</Prefix><Marker></Marker><MaxKeys>250</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated></ListBucketResult>
"""

result_acl = """<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy>
  <Owner>
    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    <DisplayName>mtd@amazon.com</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>mtd@amazon.com</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
"""


def return_xml(request):
    prefix = request.GET.get('prefix')
    if prefix.endswith(".sam") or prefix.endswith('.bam') or prefix.endswith('.fastq.gz') or prefix.endswith('.fasta') \
            or prefix.endswith('.bai') or prefix.endswith('.metrics') or prefix.endswith('.table') \
            or prefix.endswith('.fai') or prefix.endswith('.fai') or prefix.endswith('.dict') or prefix.endswith('.list') \
            or prefix.endswith('.idx') or prefix.endswith('.vcf') or prefix.endswith('.'):
        return HttpResponse(exist_result % (prefix, prefix), content_type="application/xml")
    else:
        return HttpResponse(empty, content_type="application/xml")

def acl(request, dir, name):
    return HttpResponse(result_acl, content_type="application/xml")


def put_directory(request, dir, job_id):
    return JsonResponse({}, content_type="application/json")


def put_file(request, dir, job_id, file_name):
    if request.method == 'GET':
        if file_name == '.exitcode':
            return HttpResponse(0)
        if 'acl' in request.GET:
            return HttpResponse(result_acl, content_type="application/xml")
    return JsonResponse({}, content_type="application/json")
