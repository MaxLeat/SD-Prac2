import pywren_ibm_cloud as pywren
from cos_backend import COSBackend
import pickle
import time
import datetime

N_SLAVES = 5
nom_cos = 'sdurv'
fitxer = 'p_write_'
TIME = 1


def master(id, x, ibm_cos):
    time.sleep(6)
    objectes = True
    i = 0
    data = ''
    fitxer = ""
    write_permission_list = []
    llista = []
    continguts = []
    diccionari = {}
   
   #Guardem al moment de creació del result
    ibm_cos.put_object(Bucket=nom_cos, Key='result.txt',
                       Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
    json = ibm_cos.list_objects(Bucket=nom_cos, Prefix='result')
    # Agafem el temps i eliminem la zona horaria.
    json_time = json['Contents'][0]['LastModified'].replace(tzinfo=None)
    temps_anterior=json_time
    llista = ibm_cos.list_objects(Bucket=nom_cos, Prefix='p_write')
    try:
        continguts = llista['Contents']  # Agafem els valors de "Contents"
    except:
        pass
    # 1. monitor COS bucket each X seconds
    while objectes:

        # 2. List all "p_write_{id}" files
        try:
            llista = ibm_cos.list_objects(Bucket=nom_cos, Prefix='p_write')
            continguts = llista['Contents']  # Agafem els valors de "Contents"
            i = 0
            while (i < len(continguts)):  # Llegim els Contents de tots els fitxer que hem obtingut
                # Guardem la ultima data de modificacio a Dates
                dates = []
                dates.append(continguts[i]['LastModified'])
                # Creem un diccionari amb les Dates : Nom del fitxer
                diccionari[continguts[i]['LastModified']] = continguts[i]['Key']
                i = i+1

            # 3. Order objects by time of creation
            dates.sort()  # Ordenem el vector de les dates

            # 4. Pop first object of the list "p_write_{id}"
            pop = dates.pop(0)
            # Obtenim el nom del primer fitxer ordenat.
            fitxer = str(diccionari[pop])

            # 5. Write empty "write_{id}" object into COS
            # Separem el fitxer del cos i el separem per "_"
            id = fitxer.split("_", 1)
            nom_fitxer = id[1] # Ens quedem amb la segona part "write_{id}"
            ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                            Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

            # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            ibm_cos.delete_object(Bucket=nom_cos, Key=fitxer)
            # Posem el ID a write_permission_list
            write_permission_list.append(nom_fitxer.split("_")[1])

            # 7. Monitor "result.json" object each X seconds until it is updated
            updated = False
            while(updated == False):
                time.sleep(TIME)
                # Utilitzem aquesta funcio ja que ens dona els milisegons, cosa que el get_object no
                json = ibm_cos.list_objects(Bucket=nom_cos, Prefix='result')
                # Agafem el temps i eliminem la zona horaria.
                json_time = json['Contents'][0]['LastModified'].replace(
                    tzinfo=None)
                if (temps_anterior < json_time):
                    updated=True
                    temps_anterior = json_time
            

            # 8. Delete from COS “write_{id}”
            ibm_cos.delete_object(Bucket=nom_cos, Key=nom_fitxer)
        except:
            objectes = False
       
        time.sleep(TIME)
        # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
    # Tinc posat aixo temporalment per veure que agafa el fitxer correcte
    return write_permission_list


def slave(id, x, ibm_cos):

    data = ''
    # 1. Write empty "p_write_{id}" object into COS
    nom_fitxer =  'p_write_{' + str(id) + '}'
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,
                       Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    nom_fitxer =  'write_{' + str(id) + '}'
    disponible = False
    while (disponible == False):
        try:
            temporal = ibm_cos.get_object(Bucket=nom_cos, Key=nom_fitxer)[
                'Body'].read()
            disponible = True
        except:
            disponible= False
        time.sleep(TIME)

    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    resultat = ibm_cos.get_object(Bucket=nom_cos, Key='result.txt')[
        'Body'].read()
    resultat = pickle.loads(resultat)
    resultat = resultat + str(id) + ' '
    ibm_cos.put_object(Bucket=nom_cos, Key='result.txt',
                       Body=pickle.dumps(resultat, pickle.HIGHEST_PROTOCOL))
    
    # 4. Finish

if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    actual = datetime.datetime.now()
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    print(write_permission_list)

    # Get result.txt
    cos = COSBackend()
    results = cos.get_object('sdurv', 'result.txt')
    results = pickle.loads(results)
    print(results)

    # check if content of result.txt == write_permission_list
