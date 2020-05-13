import pywren_ibm_cloud as pywren
from cos_backend import COSBackend
import pickle
import time

N_SLAVES = 1
nom_cos = 'ramonsd'
fitxer = 'p_write_'
TIME = 5


def master(id, x, ibm_cos):
    i=0
    write_permission_list = []
    llista = []
    continguts = []
    dates = [1,2,3]
    diccionari = {}

    # while (N_SLAVES != 1):
    #    time.sleep(TIME)
    # 1. monitor COS bucket each X seconds
    # 2. List all "p_write_{id}" files
    llista = ibm_cos.list_objects(Bucket=nom_cos, Prefix='Matriu')
    continguts = llista['Contents']
    while (i < len(continguts)):
        dates[i]=continguts[i]['LastModified']
        diccionari [continguts[i]['LastModified']] = continguts[i]['Key']
        i=i+1
    # 3. Order objects by time of creation    
    dates.sort()
    llista=diccionari[dates[0]]
    # 4. Pop first object of the list "p_write_{id}"
    # 5. Write empty "write_{id}" object into COS
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    # 7. Monitor "result.json" object each X seconds until it is updated
    # 8. Delete from COS “write_{id}”
    # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
    return llista


def slave(id, x, ibm_cos):
    data = ''
    # 1. Write empty "p_write_{id}" object into COS
    nom_fitxer = fitxer + '{' + '0' + '}'
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                       Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    nom_fitxer = fitxer + '{' + '0' + '}'
    disponible = False
    while (disponible == False):
        time.sleep(5)
        try:
            temporal = ibm_cos.get_object(Bucket=nom_cos, Key=nom_fitxer)[
                                          'Body'].read()
            disponible = True
            break
        except Exception:
            pass

    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    # resultat = ibm_cos.get_object(Bucket=nom_cos, Key='result.txt')['Body'].read()
    # resultat = pickle.loads(resultat)
    # resultat = resultat + str(id) + ' '
    # ibm_cos.put_object(Bucket=nom_cos, Key='result.txt',Body=pickle.dumps(resultat, pickle.HIGHEST_PROTOCOL))
    # 4. Finish


if __name__ == '__main__':
    
    i = 0
    j=2
    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    print (write_permission_list)
    # Get result.txt
    cos=COSBackend()
    results=cos.get_object('sdurv', 'result.txt')
    results=pickle.loads(results)
    print(results)
    # check if content of result.txt == write_permission_list