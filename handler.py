import redis_methods as rm
import datacreator as dc
choice = 0
while choice!=8:
    print("Welcome to redis handler. Choose the prefered action by pressing the number and the enter key.")
    print("0.Create data")
    print("1.Create a KL store")
    print("2. Filter Existing KL store")
    print("3. Apply function to existing KL store")
    print("4. Aggregate an existing KL store")
    print("5. Apply a lookup on an existing KL store")
    print("6. Clear redis data")
    print("7. Make my own query")
    print("8. Exit")
    choice = int(input("Press the number of the function code you wish to complete, and then press enter.\n"))
    if choice == 0:
        print("Note that the insertion of data into a db requires the correct configuration in the sourcefile datacreator.py")
        dbcr = input("In case you have set your db credentials, press Y/y. Press any other key to proceed with excel/csv creation\n")
        dc.datacreation(dbcr)
    elif choice == 1:
        rm.create_klstore('csv1', 'csvsource.xml', None, 0, 1, 1)
        rm.create_klstore('csv2', 'csvsource.xml', None, 0, 1, 2)
        rm.create_klstore('excel1', 'excelsource.xml', 0, 0, 1, 2)
        # create_klstore('db1', 'dbsource.xml', 'SELECT * FROM redistable', 0, 1, 1)
    elif choice == 2:
        rm.filter_klstore('csv2', 'len(value[4:]>1')
        rm.filter_klstore('csv2', 'value=="tran16"')
    elif choice == 3:
        rm.apply_klstore('csv2', 'upper_wrapper')
        rm.apply_klstore('csv2', 'letter_stripper')

    elif choice == 4:
        rm.aggr_klstore('csv2', 'count', None)
        rm.aggr_klstore('csv2', 'avg', None)
        rm.aggr_klstore('csv2', 'count', None)

    elif choice == 5:
        rm.lookup_klstore('csv1', 'excel1')

    elif choice == 6:
        rm.redis_clear()

    elif choice == 7:
        print("As proof of concept, only KL store creation is supported.")
        print("For any other functionality, feel free to modify the parameters of the auto-calls from the handler code.")
        name = input("Please set a name for the KL store:\n")
        ds = input("Please specify the xml file to be used for the KL store creation:\n")
        qs = input("Please provide the required query (sheet index if source is Excel, SQL query if source is a db. Leave blank if source is csv:\n")
        p1 = input("Please specify the column index of customers. Leave blank if source is DB:\n")
        p2 = input("Please specify the column index of transactions. Leave blank if source is DB:\n")
        d = input("Please specify wether transactions or customers will be the keys to the KL store:\n")
        rm.create_klstore(name, ds, qs, p1, p2, d)



