N_SLAVES = 10
nom_cos='sdurv'
fitxer="p_write_"

def master(id, x, ibm_cos):
    write_permission_list = []
    # 1. monitor COS bucket each X seconds
    # 2. List all "p_write_{id}" files
    # 3. Order objects by time of creation
    # 4. Pop first object of the list "p_write_{id}"
    # 5. Write empty "write_{id}" object into COS
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    # 7. Monitor "result.json" object each X seconds until it is updated
    # 8. Delete from COS “write_{id}”
    # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
    return write_permission_list

def slave(id, x, ibm_cos):
    data=''
    # 1. Write empty "p_write_{id}" object into COS
    nom_fitxer= fitxer + '{' + str(id) + '}'
    ibm_cos.put_object(Bucket=nom_cos, Key=nom_fitxer,Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    while (disponible = False):
        time.sleep(2)
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    # 4. Finish
    # No need to return anything
    
if __name__ == '__main__':
 pw = pywren.ibm_cf_executor()
 pw.call_async(master, 0)
 pw.map(slave, range(N_SLAVES))
 write_permission_list = pw.get_result()

 # Get result.txt
 # check if content of result.txt == write_permission_list