import pywren_ibm_cloud as pywren
from cos_backend import COSBackend
import pickle
import time

N_SLAVES = 1
nom_cos = 'ramonsd'
fitxer = 'p_write_'
TIME = 5


def master(id, x, ibm_cos):
    i = 0
    data=''
    fitxer = ""
    write_permission_list = []
    llista = []
    continguts = []
    # Aixo nose com fer una llista sense valors, ja que em dona error al bucle d'abaix de que esta fora de rango
    dates = [1, 2]
    diccionari = {}

    # Agafo tots els fitxers del meu cos que comencin per Matriu (Aprofitant la practica 1)
   
    # 1. monitor COS bucket each X seconds
    #while (i==0):
    # --------------------------------------------------------- INDENTAR ------------------------------------------------------------
    time.sleep(TIME)

    # 2. List all "p_write_{id}" files
    llista = ibm_cos.list_objects(Bucket=nom_cos, Prefix='p_write')
    continguts = llista['Contents']  # Agafem els valors de "Contents"
    while (i < len(continguts)):  # Llegim els Contents de tots els fitxer que hem obtingut
        # Guardem la ultima data de modificacio a Dates
        dates[i] = continguts[i]['LastModified']
        # Creem un diccionari amb les Dates : Nom del fitxer
        diccionari[continguts[i]['LastModified']] = continguts[i]['Key']
        i = i+1

    # 3. Order objects by time of creation
    dates.sort()  # Ordenem el vector de les dates
    

    # 4. Pop first object of the list "p_write_{id}"
    pop = dates.pop(0)

    # Obtenim el nom del primer fitxer ordenat.
    fitxer = str(diccionari[pop])

    # 5. Write empty "write_{id}" objsiect into COS
    # Separem el fitxer del cos i el separem per "_"
    id = fitxer.split("_", 1)
    nom_fitxer = id[1]  # Ens quedem amb la segona part "write_{id}"
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                        Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    #ibm_cos.delete_object(Bucket=nom_cos, Key=fitxer) AIXO NO FUNCIONA NO SE PERQUE
    write_permission_list.append(nom_fitxer.split("_")[1])
    # 7. Monitor "result.json" object each X seconds until it is updated

    # 8. Delete from COS “write_{id}”
    # Agafo tots els fitxers del meu cos que comencin per Matriu (Aprofitant la practica 1)
    

    # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
    # --------------------------------------------------------- INDENTAR ------------------------------------------------------------
    return fitxer  # Tinc posat llista temporalment per veure que agafa el fitxer correcte


def slave(id, x, ibm_cos):
    data = ''
    # 1. Write empty "p_write_{id}" object into COS
    nom_fitxer = fitxer + '{' + '0' + '}'
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                       Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
    nom_fitxer = fitxer + '{' + '1' + '}'
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                       Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))                   

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    nom_fitxer = fitxer + '{' + '0' + '}'
    disponible = False
    while (disponible == False):
        time.sleep(TIME)
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
    llista_prova=[1,2,3]
    nova_llista=[]
    i = 0
    j = 2
    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    print(write_permission_list)
    # Get result.txt
    cos = COSBackend()
    results = cos.get_object('sdurv', 'result.txt')
    results = pickle.loads(results)
    print(results)
    # check if content of result.txt == write_permission_list
