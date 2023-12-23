import os

def createDataWarehouse(inputFolder,outputFile):
    folders = [x[0] for x in os.walk(inputFolder)]
    folders.remove(inputFolder)

    warehouseTablesValues = createDataWarehouseLists(folders)
    print("- warehouseTablesValues created")

    sql = createSql(warehouseTablesValues)
    print("- sql variable created")

    lines = sql.split("\n")
    lines = convert_to_postgres(lines)

    f = open(outputFile, "w")
    f.writelines(lines)
    f.close()
    print("- sql File created")

def createDataWarehouseLists(folders):

    # tables with usefull/necessary informations
    tables = ["meet","athlete","club","swimstyle","session","result","event","agegroup","ranking"]
    tablesDic={
        "meet":["meetid","name","city","nation","course"],
        "session":["sessionid","meetid","name","date"],
        "club":["code","name","nation","region","clubid","meetid"],
        "result":["resultid","eventid","points","swimtime","athleteid"],
        "athlete":["license","firstname","lastname","gender","birthdate","nation","clubid","athleteid"],
        "swimstyle":["swimstyleid","distance","stroke","relaycount","eventid","swimstylecode"],
        "event":["eventid","gender","sessionid"],
        "agegroup":["agegroupid","agemin","agemax"],
        "ranking":["resultid","agegroupid","place"]}
    tablesValues={
        "meet" : [],
        "session" : [],
        "club" : [],
        "athlete" : [],
        "swimstyle" : [],
        "result" : [],
        "event" : [],
        "agegroup" : [],
        "ranking" : []}

    # tables for the data Warehouse
    #warehouseTables = ["fact","meet","athlete","club","swimstyle"]
    #warehouseTablesDic={
    #    "fact":["license","code","meetid","swimstylecode","points","swimtime","place"],
    #    "meet":["meetid","name","city","nation","course","startdate","enddate"],
    #    "club":["code","name","nation","region"],
    #    "athlete":["license","firstname","lastname","gender","birthdate","nation"],
    #    "swimstyle":["swimstylecode","distance","stroke","relaycount"],
    #    "ageGroup":["agegroupid","minage","maxage"]}
    warehouseTablesValues={
        "fact" : [],
        "meet" : [],
        "club" : [],
        "athlete" : [],
        "swimstyle" : [],
        "ageGroup": []}

    # meetid which is 41231234, 41331234, 41431234 ...
    id_key = 41231234
    c = 0

    for folder in folders:
        files = os.listdir(folder)
        file = [f for f in files if f.endswith(".sql")][0]
        sqlFile = open(folder+"/"+file,"r")
        lines = sqlFile.readlines()
        for line in lines:
            if line.startswith("INSERT IGNORE INTO") or line.startswith("-- INSERT IGNORE INTO"):
                newLine = line.split("`.`")[1].split("VALUES")
                table = newLine[0].split("`")[0]
                if table in tables:
                    keys = [i.replace("`","").replace(" ","") for i in newLine[0].split("(")[1].split(")")[0].split(",")]
                    values = newLine[1].split("(")[1].split(")")[0].split("'")
                    values = [i for i in values if any(char.isdigit() for char in i) or any(c.isalpha() for c in i) ]
                    v2 = values.copy()
                    values = []
                    for i in v2:
                        if i.startswith(" "):
                            values.append(i[1:])
                        elif i.endswith(" "):
                            values.append(i[:-1])
                        else:
                            values.append(i)
                    dic = {keys[i]: values[i] for i in range(len(values))}
                    # some results didn`t had um athleteid or license só we didn`t consider them
                    # if there are no points in the result we didn`t consider it
                    # if there is no eventid in swimstyle we didn`t consider it
                    if (table == "result" and "athleteid" not in dic) or \
                            (table == "swimstyle" and "eventid" not in dic) or \
                            (table == "result" and "points" not in dic):
                        continue
                    li = []
                    if table == "swimstyle":
                        for i in tablesDic[table][:-1]:
                            li = li + [dic[i]]
                        # [str(li[1])+str(li[2]).upper()+str(li[3])] creates an unique key for the swimstyle
                        li = li + [str(li[1])+str(li[2]).upper()+str(li[3])]
                    elif "meetid" in tablesDic[table]:
                        for i in tablesDic[table]:
                            if i=="meetid":
                                li = li + [id_key+c]
                            else:
                                li = li + [dic[i]]
                    elif "license" in tablesDic[table]:
                        for i in tablesDic[table]:
                            if i=="license":
                                if "license" in dic:
                                    li = li + [dic["license"]]
                                else:
                                    # creation of license if it does not exist
                                    li = li + [str(id_key+c) + str(dic["clubid"]) + str(dic["athleteid"])]
                            else:
                                li = li + [dic[i]]
                    else:
                        li = [dic[i] for i in tablesDic[table]]
                    tablesValues[table]=tablesValues[table]+[li]

        # Adding formated data to warehouseTablesValues for the meeting
        agegroupDic = {"A": ['25', '29'],
                       "B": ['30', '34'],
                       "C": ['35', '39'],
                       "D": ['40', '44'],
                       "E": ['45', '49'],
                       "F": ['50', '54'],
                       "G": ['55', '59'],
                       "H": ['60', '64'],
                       "I": ['65', '69'],
                       "J": ['70', '74'],
                       "K": ['75', '79'],
                       "L": ['80', '84'],
                       "M": ['85', '89'],
                       "N": ['90', '94'],
                       "O": ['95', '99']}
        for res in tablesValues["result"]:
            points = res[2]
            swimtime = res[3]
            meetid = id_key+c
            athleteid = res[4]
            license = [a[0] for a in tablesValues["athlete"] if a[7]==athleteid][0]
            clubid = [a[6] for a in tablesValues["athlete"] if a[7]==athleteid][0]
            code = [a[0] for a in tablesValues["club"] if a[4]==clubid][0]
            eventid = res[1]
            place = [a[2] for a in tablesValues["ranking"] if a[0]==res[0]][0]
            swimstylecode = [a[5] for a in tablesValues["swimstyle"] if a[4]==eventid][0]

            resid = res[0]
            rank = [i for i in tablesValues["ranking"] if resid == i[0]][0]
            aG = [i for i in tablesValues["agegroup"] if rank[1] == i[0]][0]
            interv = [str(aG[1]), str(aG[2])]
            ageGroupid = ""
            for k in list(agegroupDic.keys()):
                if agegroupDic[k] == interv:
                    ageGroupid = k
                    break
            if ageGroupid == "":
                print(interv, "agegroup does not exists")
            warehouseTablesValues["ageGroup"] = warehouseTablesValues["ageGroup"]+[[ageGroupid,interv[0],interv[1]]]

            warehouseTablesValues["fact"] = warehouseTablesValues["fact"] + [[license,code,meetid,swimstylecode,ageGroupid,points,swimtime,place]]
        for ath in tablesValues["athlete"]:
            warehouseTablesValues["athlete"] = warehouseTablesValues["athlete"] + [ath[0:6]]
        for clu in tablesValues["club"]:
            warehouseTablesValues["club"] = warehouseTablesValues["club"] + [clu[0:4]]
        for swi in tablesValues["swimstyle"]:
            warehouseTablesValues["swimstyle"] = warehouseTablesValues["swimstyle"] + [[swi[1],swi[2],swi[3],swi[5]]]
        for me in tablesValues["meet"]:
            dates = [i[3] for i in tablesValues["session"] if i[1]==me[0]]
            warehouseTablesValues["meet"] = warehouseTablesValues["meet"] + [me+[min(dates),max(dates)]]

        tablesValues = {
            "meet": [],
            "session": [],
            "club": [],
            "athlete": [],
            "swimstyle": [],
            "result": [],
            "event": [],
            "agegroup":[],
            "ranking":[]}
        c = c + 100000

    # dropping of duplicated athetes, clubs and swimstyles
    ath = warehouseTablesValues["athlete"]
    clu = warehouseTablesValues["club"]
    swi = warehouseTablesValues["swimstyle"]
    age = warehouseTablesValues["ageGroup"]
    li = []
    athletes=[]
    for i in ath:
        if i[0] not in li:
            athletes.append(i)
            li.append(i[0])
    warehouseTablesValues["athlete"] = athletes
    co = []
    clubs = []
    for i in clu:
        if i[0] not in co:
            clubs.append(i)
            co.append(i[0])
    warehouseTablesValues["club"] = clubs
    sw = []
    swimstyles = []
    for i in swi:
        if i[3] not in sw:
            swimstyles.append(i)
            sw.append(i[3])
    warehouseTablesValues["swimstyle"] = swimstyles
    ag = []
    ageGroups = []
    for i in age:
        if i[0] not in ag:
            ageGroups.append(i)
            ag.append(i[0])
    warehouseTablesValues["ageGroup"] = ageGroups
    return warehouseTablesValues

def createSql(warehouseTablesValues):
    sql = ""
    sql += creating_database("dataWarehouse")
    sql += create_table_structure()
    sql += create_realtions()
    sql += myGenerate_sql_meets(warehouseTablesValues["meet"])
    sql += myGenerate_sql_clubs(warehouseTablesValues["club"])
    sql += myGenerate_sql_athletes(warehouseTablesValues["athlete"])
    sql += myGenerate_sql_swimstyle(warehouseTablesValues["swimstyle"])
    sql += myGenerate_sql_agegroups(warehouseTablesValues["ageGroup"])
    sql += myGenerate_sql_facts(warehouseTablesValues["fact"])
    return sql

def creating_database(name):
    sql = ""
    sql += "--DROP DATABASE IF EXISTS " + name + ";\n"
    sql += "--CREATE DATABASE "+ name + ";\n"
    return sql
def create_table_structure():
    tables = ["fact","swimstyle","meet","athlete","club","agegroup"]
    sql = ""
    for i in tables:
        sql += "DROP TABLE IF EXISTS "+i+";\n"
        if i=="fact":
            sql += "CREATE TABLE fact (license VARCHAR(25) NOT NULL, code VARCHAR(25) NOT NULL, meetid VARCHAR(25) NOT NULL, swimstylecode VARCHAR(25) NOT NULL, agegroupid VARCHAR(25) NOT NULL, points INT, swimtime TIME, place INT, PRIMARY KEY (license, code, meetid, swimstylecode,agegroupid));\n"
        elif i=="meet":
            sql += "CREATE TABLE meet (meetid VARCHAR(25) NOT NULL, name VARCHAR(75), course VARCHAR(25), city VARCHAR(25), nation VARCHAR(25), startdate DATE, enddate DATE, PRIMARY KEY (meetid));\n"
        elif i=="swimstyle":
            sql += "CREATE TABLE swimstyle (swimstylecode VARCHAR(25) NOT NULL, distance INT, stroke VARCHAR(25), relaycount VARCHAR(25), PRIMARY KEY (swimstylecode));\n"
        elif i=="athlete":
            sql += "CREATE TABLE athlete (license VARCHAR(25) NOT NULL, firstname VARCHAR(50), lastname VARCHAR(25), gender VARCHAR(25), birthdate DATE, nation VARCHAR(25), PRIMARY KEY (license));\n"
        elif i=="club":
            sql += "CREATE TABLE club (code VARCHAR(25) NOT NULL, name VARCHAR(50), nation VARCHAR(25), region VARCHAR(25), PRIMARY KEY (code));\n"
        elif i == "agegroup":
            sql += "CREATE TABLE agegroup (agegroupid VARCHAR(25) NOT NULL, minage INT, maxage INT, PRIMARY KEY (agegroupid));\n"
    return sql
def create_realtions():
    sql=""
    sql += "ALTER TABLE fact ADD CONSTRAINT meetmeetidfact FOREIGN KEY (meetid) REFERENCES meet (meetid) ON DELETE NO ACTION ON UPDATE NO ACTION;\n"
    sql += "ALTER TABLE fact ADD CONSTRAINT swimstyleswimstylecodefact FOREIGN KEY (swimstylecode) REFERENCES swimstyle (swimstylecode) ON DELETE NO ACTION ON UPDATE NO ACTION;\n"
    sql += "ALTER TABLE fact ADD CONSTRAINT athleteathleteidfact FOREIGN KEY (license) REFERENCES athlete (license) ON DELETE NO ACTION ON UPDATE NO ACTION;\n"
    sql += "ALTER TABLE fact ADD CONSTRAINT clubclubidfact FOREIGN KEY (code) REFERENCES club (code) ON DELETE NO ACTION ON UPDATE NO ACTION;\n"
    sql += "ALTER TABLE fact ADD CONSTRAINT agegroupagegroupidfact FOREIGN KEY (agegroupid) REFERENCES agegroup (agegroupid) ON DELETE NO ACTION ON UPDATE NO ACTION;\n"
    return sql

def myGenerate_sql_meets(meets):
    sql = ''
    for m in meets:
        sql += "INSERT IGNORE INTO `annp_final`.`meet` (`meetid`, `name`, `city`, `nation`,`course`, `startdate`, `enddate`) VALUES ('"
        sql += str(m[0])
        sql += "', '"
        sql += m[1]
        sql += "', '"
        sql += m[2]
        sql += "', '"
        sql += m[3]
        sql += "', '"
        sql += m[4]
        sql += "', '"
        sql += m[5]
        sql += "', '"
        sql += m[6]
        sql += "');"
        sql += "\n"

    return sql
def myGenerate_sql_facts(facts):
    sql = ''
    for s in facts:
        sql += "INSERT IGNORE INTO `annp_final`.`fact` (`license`, `code`, `meetid`, `swimstylecode`, `agegroupid`, `points`, `swimtime`, `place`) VALUES ('"
        sql += s[0]
        sql += "', '"
        sql += s[1]
        sql += "', '"
        sql += str(s[2])
        sql += "', '"
        sql += s[3]
        sql += "', '"
        sql += s[4]
        sql += "', '"
        sql += str(s[5])
        sql += "', '"
        sql += s[6]
        sql += "', '"
        sql += str(s[7])
        sql += "');"
        sql += "\n"
    return sql
def myGenerate_sql_clubs(clubs):
    sql = ''
    for c in clubs:
        sql += "INSERT IGNORE INTO `annp_final`.`club` (`code`, `name`, `nation`, `region`) VALUES ('"
        sql += c[0]
        sql += "', '"
        sql += c[1]
        sql += "', '"
        sql += c[2]
        sql += "', '"
        sql += c[3]
        sql += "');"
        sql += "\n"
    return sql
def myGenerate_sql_athletes(athletes):
    sql = ''
    for a in athletes:
        sql += "INSERT IGNORE INTO `annp_final`.`athlete` (`license`, `firstname`, `lastname`, `gender`, `birthdate`, `nation`) VALUES ('"
        sql += a[0]
        sql += "', '"
        sql += a[1]
        sql += "', '"
        sql += a[2]
        sql += "', '"
        sql += a[3]
        sql += "', '"
        sql += a[4]
        sql += "', '"
        sql += a[5]
        sql += "');"
        sql += "\n"
    return sql
def myGenerate_sql_swimstyle(swimstyle):
    sql = ''
    for s in swimstyle:
        sql += "INSERT IGNORE INTO `annp_final`.`swimstyle` (`swimstylecode`, `distance`, `stroke`, `relaycount`) VALUES ('"
        sql += s[3]
        sql += "', '"
        sql += s[0]
        sql += "', '"
        sql += s[1]
        sql += "', '"
        sql += s[2]
        sql += "');"
        sql += "\n"
    return sql
def myGenerate_sql_agegroups(agegroups):
    sql = ''
    for s in agegroups:
        sql += "INSERT IGNORE INTO `annp_final`.`agegroup` (`agegroupid`, `minage`, `maxage`) VALUES ('"
        sql += s[0]
        sql += "', '"
        sql += str(s[1])
        sql += "', '"
        sql += str(s[2])
        sql += "');"
        sql += "\n"
    return sql

def convert_to_postgres(lines):
    lines = [line.replace("`", "") for line in lines]
    lines = [line.replace("IGNORE", "") for line in lines]
    lines = [line.replace("db_annp.", "") for line in lines]
    lines = [line.replace("annp_final.", "") for line in lines]
    lines = [line + "\n" for line in lines]
    return lines

if __name__ == '__main__':
    inputFolder = "files"
    outputFile = "dataWarehousePostgres.sql"
    createDataWarehouse(inputFolder,outputFile)

