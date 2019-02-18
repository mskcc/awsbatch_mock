import json
import manage
from django.http import JsonResponse
import batch.mock_executor.mock_executor as mock_executor

# Create your views here.
def upload_s3(request, root_id, job_name, file_name):
    if request.method == 'PUT':
        return JsonResponse({})
    elif request.method == 'GET':
        return download_s3(job_name)


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
    return JsonResponse({})


def register_job_definition(request):
    body = json.loads(request.body)
    job_name = body['jobDefinitionName']
    name = body['containerProperties']['command'][8].split('/')[-1]
    manage.job_service.start_job(job_name, name)
    return JsonResponse({})


def list_jobs(request):
    result = []
    body = json.loads(request.body)
    if body['jobStatus'] == 'SUCCEEDED':
        for completed in mock_executor.completed:
            result.append(completed.dump())
        return JsonResponse({"jobSummaryList": result}, content_type="application/json")
    else:
        return JsonResponse({"jobSummaryList": []}, content_type="application/json")
