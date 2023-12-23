from cmath import log
import os
import shlex
import sys
import subprocess
import xmltodict, json

def get_files_path(path, extendion='lxf'):
    for (_, _, filenames) in os.walk(path):
        return [path+'/'+file_name for file_name in filenames if '.'+extendion in file_name]
    
def unzip_files(files_path):
    folders_path = []
    for file_path in files_path:
        folder_path = file_path[:-4]
        folders_path.append(folder_path)
        subprocess.Popen(['unzip', '-u', '-d', folder_path, file_path]).wait()
        
    return folders_path
        
def xml_to_sql(folders_path):
    json_files = []
    json_file_path = []
    global_json = {
        "MEETS": {
            "MEET": []
        }
    }
    
    for folder_path in folders_path:
        files_path = get_files_path(folder_path, 'lef')
        json_files += xml_to_json(files_path)
        json_file_path += files_path
    
    for json_file, file_path in zip(json_files, json_file_path):
        file_name = '.'.join(file_path.split('/')[-1].split('.')[:-1])
        if 'LENEX' in json_file and 'MEETS' in json_file['LENEX'] and 'MEET' in json_file['LENEX']['MEETS']:
            global_json['MEETS']['MEET'].append(json_file['LENEX']['MEETS']['MEET'])
        json_to_sql(json_file, file_name, '/'.join(file_path.split('/')[:-1]))
     
    # CREATE ANNP_FINAL_DATA
    id_key = 41231234
    for json_file, file_path in zip(json_files, json_file_path):
        sql_annp_final_inputs = create_annp_final_inputs(json_file['LENEX']["MEETS"]['MEET'], id_key)
        folder_path = '/'.join(file_path.split('/')[0:-1])
        save_annp_final_inputs_sql(sql_annp_final_inputs, folder_path)
    
    save_global_json(global_json)
    json_to_sql(global_json, 'annp', './')
    
def save_annp_final_inputs_sql(sql, folder_path):
    f = open(folder_path+'/annp_final_inputs.sql', "w")
    f.write(sql)
    f.close()

def save_global_json(global_json):
    with open('./annp.json', 'w') as f:
        json.dump(global_json, f, indent=2)
        print("Save global json file")
        
def xml_to_json(files_path):
    json_files = []
    for file_path in files_path:
        with open(file_path) as f:
            xml = xmltodict.parse(f.read())
            json_file = json.loads(json.dumps(xml))
            json_files.append(json_file)
            # print(json_files)
        with open(file_path+'.json', 'w') as f:
            json.dump(json_file, f, indent=2)
            print("New json file is created from data.json file")
    return json_files

def json_to_sql(data, file_name, path):
    db = Database(file_name, data)
    f = open(path+'/'+file_name+'.sql', "w")
    f.write(str(db))
    f.close()
    
def create_annp_final_inputs(data, id_key):
    results = []
    athletes = []
    clubs = []
    enrolls = []
    splits = []
    sessions = []
    events = []
    swimstyle = {}
    
    data['@meetid'] = id_key
    meets = [{
        'meetid': data['@meetid'],
        'name': data['@name'],
        'course': data['@course'],
        'city': data['@city'],
        'nation': data['@nation'],
        'poolid': data['@poolid'] if '@poolid' in data else id_key
        },]
    
    # CLUBS
    for club in data['CLUBS']['CLUB']:
        clubs.append({'clubid': club['@clubid'], 'code': club['@code'], 'name': club['@name'], 'nation': club['@nation'], 'region': club['@region'], }) 
        if 'ATHLETES' not in club:
            continue
        if 'ATHLETE' not in club["ATHLETES"]:
            continue
        if type(club['ATHLETES']['ATHLETE']) != type([]):
            club['ATHLETES']['ATHLETE'] = [club['ATHLETES']['ATHLETE']]
        
        # ATHLETES
        for idx, athlete in enumerate(club['ATHLETES']['ATHLETE']):
            license = athlete['@license'] if '@license' in athlete else str(int(id_key) + int(club['@clubid']) + idx)
            athletes.append({
                'license': license,
                'firstname': athlete['@firstname'],
                'lastname': athlete['@lastname'],
                'gender': athlete['@gender'],
                'birthdate': athlete['@birthdate'],
                'nation': athlete['@nation'],
            })
        
            # ENROLLS
            enrolls.append({
                'meetid': data['@meetid'],
                'license':license,
                'clubid': club['@clubid'],
            })

            # RESULTS
            if 'RESULTS' not in athlete:
                continue
            
            if 'RESULT' not in athlete['RESULTS']:
                continue
            
            if '@resultid' in athlete['RESULTS']['RESULT']:
                athlete['RESULTS']['RESULT'] = [athlete['RESULTS']['RESULT']]
            
            for result in athlete['RESULTS']['RESULT']:
                results.append({
                    'resultid': result['@resultid'],
                    'eventid': result['@eventid'],
                    'license': license,
                    'state': result['@state'] if '@state' in result else '',
                    'points': result['@points'] if '@points' in result else '',
                    'swimtime': result['@swimtime'],
                })
                
            if 'SPLITS' not in result:
                continue
            
            if 'SPLIT' not in result['SPLITS']:
                continue
            
            # SPLITS
            if '@distance' in result['SPLITS']['SPLIT'] or '@swimtime' in result['SPLITS']['SPLIT']:
                result['SPLITS']['SPLIT'] = [result['SPLITS']['SPLIT']]

            for split in result['SPLITS']['SPLIT']:
                splits.append({
                    'splitid': str(id_key + len(splits) + 1),
                    'resultid': result['@resultid'],
                    'distance': split['@distance'],
                    'time': split['@swimtime'],
                })

    # SESSIONS
    for session in data['SESSIONS']['SESSION']:
        sessionid = id_key + len(session) + 1
        sessions.append({
            'sessionid': str(sessionid),
            'name': session['@name'],
            'date': session['@date'],
            'time': session['@daytime'],
            'meetid': data['@meetid'],
        })
        
        if 'EVENTS' not in session:
            continue
        if 'EVENT' not in session['EVENTS']:
            continue
        if '@eventid' in session['EVENTS']['EVENT']:
            session['EVENTS']['EVENT'] = [session['EVENTS']['EVENT']]

        # EVENT
        for event in session['EVENTS']['EVENT']:
            swimstyleKey = event['SWIMSTYLE']['@distance'] + event['SWIMSTYLE']['@relaycount'] + event['SWIMSTYLE']['@stroke'];        
            if swimstyleKey not in swimstyle:
                swimstyle[swimstyleKey] = {
                    'swimstyleid': str(id_key + len(swimstyle) + 1),
                    'distance': event['SWIMSTYLE']['@distance'],
                    'stroke': event['SWIMSTYLE']['@stroke'],
                    'relaycount': event['SWIMSTYLE']['@relaycount'],
                }

            swimstyleid = swimstyle[swimstyleKey]['swimstyleid']

            events.append({
                'eventid': event['@eventid'],
                'gender': event['@gender'],
                'time': event['@daytime'] if '@daytime' in event else '' ,
                'sessionid': str(sessionid),
                'swimstyleid': swimstyleid,
            })
    sql = ''
    sql += generate_sql_annp_final_meets(meets)
    sql += generate_sql_annp_final_clubs(clubs)
    sql += generate_sql_annp_final_athletes(athletes)
    sql += generate_sql_annp_final_enrolls(enrolls)
    sql += generate_sql_annp_final_swimstyle(swimstyle)
    sql += generate_sql_annp_final_swimstyle(swimstyle)
    sql += generate_sql_annp_final_sessions(sessions)
    sql += generate_sql_annp_final_events(events)
    sql += generate_sql_annp_final_results(results)
    sql += generate_sql_annp_final_splits(splits)
    return sql
    
def generate_sql_annp_final_meets(meets):
    sql = ''
    for m in meets:
        sql +=  "INSERT IGNORE INTO `annp_final`.`meet` (`meetid`, `name`, `course`, `city`, `nation`) VALUES ('"
        sql += str(m['meetid'])
        sql += "', '"
        sql += m['name']
        sql += "', '"
        sql += m['course']
        sql += "', '"
        sql += m['city']
        sql += "', '"
        sql += m['nation']
        sql += "');"
        sql += "\n"
        
    return sql

def generate_sql_annp_final_clubs(clubs):
    sql = ''
    for c in clubs:
        sql += "INSERT IGNORE INTO `annp_final`.`club` (`clubid`, `code`, `name`, `nation`, `region`) VALUES ('"
        sql += c['clubid']
        sql += "', '"
        sql += c['code']
        sql += "', '"
        sql += c['name']
        sql += "', '"
        sql += c['nation']
        sql += "', '"
        sql += c['region']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_athletes(athletes):
    sql = ''
    for a in athletes:
        sql += "INSERT IGNORE INTO `annp_final`.`athlete` (`license`, `firstname`, `lastname`, `gender`, `birthdate`, `nation`) VALUES ('"
        sql += a['license']
        sql += "', '"
        sql += a['firstname']
        sql += "', '"
        sql += a['lastname']
        sql += "', '"
        sql += a['gender']
        sql += "', '"
        sql += a['birthdate']
        sql += "', '"
        sql += a['nation']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_enrolls(enrolls):
    sql = ''
    for e in enrolls:
        sql += "INSERT IGNORE INTO `annp_final`.`enrolls` (`meetid`, `license`, `clubid`) VALUES ('"
        sql += str(e['meetid'])
        sql += "', '"
        sql += e['license']
        sql += "', '"
        sql += e['clubid']
        sql += "');" 
        sql += "\n"
    return sql

def generate_sql_annp_final_swimstyle(swimstyle):
    sql = ''
    for key in swimstyle:
        s = swimstyle[key]
        sql += "INSERT IGNORE INTO `annp_final`.`swimstyle` (`swimstyleid`, `distance`, `stroke`, `relaycount`) VALUES ('"
        sql += s['swimstyleid']
        sql += "', '"
        sql += s['distance']
        sql += "', '"
        sql += s['stroke']
        sql += "', '"
        sql += s['relaycount']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_swimstyle(swimstyle):
    sql = ''
    for key in swimstyle:
        s = swimstyle[key]
        sql += "INSERT IGNORE INTO `annp_final`.`swimstyle` (`swimstyleid`, `distance`, `stroke`, `relaycount`) VALUES ('"
        sql += s['swimstyleid']
        sql += "', '"
        sql += s['distance']
        sql += "', '"
        sql += s['stroke']
        sql += "', '"
        sql += s['relaycount']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_sessions(sessions):
    sql = ''
    for s in sessions:
        sql += "INSERT IGNORE INTO `annp_final`.`session` (`sessionid`, `name`, `date`, `time`, `meetid`) VALUES ('"
        sql += s['sessionid']
        sql += "', '"
        sql += s['name']
        sql += "', '"
        sql += s['date']
        sql += "', '"
        sql += s['time']
        sql += "', '"
        sql += str(s['meetid'])
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_events(events):
    sql = ''
    for e in events:
        sql += "INSERT IGNORE INTO `annp_final`.`event` (`eventid`, `gender`, `time`, `sessionid`, `swimstyleid`) VALUES ('"
        sql += e['eventid']
        sql += "', '"
        sql += e['gender']
        sql += "', '"
        sql += e['time']
        sql += "', '"
        sql += e['sessionid']
        sql += "', '"
        sql += e['swimstyleid']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_results(results):
    sql = ''
    for r in results:
        sql += "INSERT IGNORE INTO `annp_final`.`result` (`resultid`, `eventid`, `license`, `state`, `points`, `swimtime`) VALUES ('"
        sql += r['resultid']
        sql += "', '"
        sql += r['eventid']
        sql += "', '"
        sql += r['license']
        sql += "', '"
        sql += r['state']
        sql += "', '"
        sql += r['points']
        sql += "', '"
        sql += r['swimtime']
        sql += "');"
        sql += "\n"
    return sql

def generate_sql_annp_final_splits(splits):
    sql = ''
    for s in splits:
        sql += "INSERT IGNORE INTO `annp_final`.`split` (`splitid`, `resultid`, `distance`, `time`) VALUES ('"
        sql += s['splitid']
        sql += "', '"
        sql += s['resultid']
        sql += "', '"
        sql += s['distance']
        sql += "', '"
        sql += s['time']
        sql += "');"
        sql += "\n"
    return sql

class Database:
    def __init__(self, name, data):
        self.name = 'db_' + name.replace(' ', '_')
        self.tables = {}
        self.read_data(data)
        self.tables_index = {}
        self.data = data
        
    def find_primary_key(self, data, name):
        primary_key_index = '@'+name.lower()+'id'
        if primary_key_index in data:
            return data[primary_key_index]
        return ''
    
    def create_insert_data_row(self, table_name, key, parent_table_name, parent_table_index, data):
        s = ''
        primary_key = self.find_primary_key(data, table_name)
                
        if primary_key == '':
            primary_key = self.tables_index[table_name]
            
        s+=self.tables[table_name].create_insert_data(primary_key, parent_table_name, parent_table_index, data, self.name)
        s+=self.create_insert_data(table_name, primary_key, data)
        self.tables_index[table_name]+=1
        return s
        
    def create_insert_data(self, parent_table_name, parent_table_index, data):
        s = ''
        for key in data:
            table_name = key.lower()
            if table_name[0] == '@':
                pass
            elif type(data[key]) == type([]):
                for d in data[key]:
                    s+=self.create_insert_data_row(table_name, key, parent_table_name, parent_table_index, d)
            elif table_name not in self.tables:
                s += self.create_insert_data(parent_table_name, parent_table_index, data[key])
            else:
                s+=self.create_insert_data_row(table_name, key, parent_table_name, parent_table_index, data[key])
        return s
        
    def read_data(self, data, parent_table_name='', h=0):
        for name in data:
            if type(data[name]) == type([]):
                for d in data[name]:
                    self.update_tables(name, parent_table_name, d)
                    self.read_data(d, name, h+1)
            elif name[0]!='@' and len(data[name])== 1:
                self.read_data(data[name], parent_table_name, h)
            elif name[0] != '@':
                self.update_tables(name, parent_table_name, data[name])
                self.read_data(data[name], name, h+1)
     
    def update_tables(self, name, parent_table, data):
        table_name = name.lower()
        parent_table_name = parent_table.lower()
        db_name = self.name
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, parent_table_name, db_name)
        
        self.tables[table_name].update(data, parent_table)
        


    def __str__(self):
#       CREATE DATABASE
        s = 'DROP DATABASE IF EXISTS `{name}`;\n'.format(name=self.name)
        s += 'CREATE DATABASE `{name}`;\n'.format(name=self.name)
    
#       CREATE TABLES
        for table in self.tables:
            s += str(self.tables[table])
            
#       CREATE FOREIGN KEYS
        for table in self.tables:
            s += self.tables[table].print_foreign_keys()
            
#       INSERT DATA
        self.tables_index = {table_name: 1 for table_name in list(self.tables)}
        s+=self.create_insert_data('', '', self.data)
            
        return s
            
class Table:
    def __init__(self, name, parent_table_name='', db_name=''):
        self.name = name
        self.parent_tables = {}
        self.columns = {}
        self.primary_key = name + 'id'
        self.db_name = db_name
        
    def update_foreign_keys(self, parent_table_name):
        if parent_table_name not in self.parent_tables and parent_table_name!='':
            name = parent_table_name.lower()+'id'
            db_name = self.db_name
            table_name = self.name
            self.parent_tables[parent_table_name] = ColumnForeignKey(name, db_name, table_name)
            self.add_column('@'+name)
    
    def update(self, data, parent_table_name):
        for column_name in data:
            if column_name == '@'+self.name+'id':
                continue
            column = self.add_column(column_name) 
            column.update(data)
        self.update_foreign_keys(parent_table_name)
        
    def add_column(self, column_name):
        foreign_key_name = column_name.lower()
        if foreign_key_name[0] != '@':
            return Column('_')
        if column_name not in self.columns:
            self.columns[foreign_key_name] = Column(foreign_key_name)
        return self.columns[foreign_key_name]
    
    def print_foreign_keys(self):
        s = ''
        for parent_table in self.parent_tables:
            s+=str(self.parent_tables[parent_table])
        return s
    
    def create_insert_data(self, primary_key, parent_table_name, parent_table_val, data, db_name):        
        columns_name = []
        columns_value = []
        
        for key in data:
            if key[0]!='@':
                pass
            elif key[1:] == self.name+'id':
                pass
            else:
                columns_name.append(key[1:])
                columns_value.append(data[key])
                
        columns_name.append(self.name+'id')
        columns_value.append(primary_key)
        
        if parent_table_name != '':
            columns_name.append(parent_table_name+'id')
            columns_value.append(parent_table_val)
        
        tables = ['athlete', 'club', 'pool', 'meet', 'session', 'event', 'result', 'split', 'swimstyle']
        s = ''
        
        if self.name not in tables:
            s+= '-- '
        
        s += 'INSERT IGNORE INTO `{db_name}`.`{name}` ('.format(db_name=db_name, name=self.name)
        
        for column_name in columns_name:
            s+='`{column_name}`, '.format(column_name=column_name)
        
        s = s[:-2] + ') VALUES ( '
        
        for column_value in columns_value:
            s+= "'{column_value}', ".format(column_value=column_value)
        
        s=s[:-2]+' );\n'
        return s
            
    def __str__(self):
        # athlete
        # club
        # pool
        # meet
        # session
        # event
        # result
        # split
        # swimstyle
        
        if self.name in ['athlete', 'club', 'pool', 'meet', 'session', 'event', 'result', 'split', 'swimstyle']:
            s = 'CREATE TABLE `{db_name}`.`{name}` ('.format(db_name = self.db_name, name=self.name)
        else:
            s = '-- CREATE TABLE `{db_name}`.`{name}` ('.format(db_name = self.db_name, name=self.name)
            
        for column in self.columns:
            s += str(self.columns[column])+', '
        s += '`{primary_key}` INT NOT NULL,PRIMARY KEY (`{primary_key2}`));\n'.format(primary_key=self.name+ 'id', primary_key2=self.name+ 'id')
        return s
    
class Column:
    def __init__(self, name):
        self.name = name[1:]
        self.varchar_length = 0
        self.set_type(type="VARCHAR" if name[-2:] != 'id' else 'INT')
        
    def update(self, data):
        if(self.name != ''):
#             print(self.name, data['@'+self.name])
            if self.name[:2] != 'id':
                self.increase_varchar_length( data['@'+self.name])
        
    def increase_varchar_length(self, str_word=''):
        while len(str_word) > self.varchar_length:
            self.varchar_length += 25
            
        self.set_type(type="VARCHAR")
        
    def set_type(self, type='VARCHAR'):
        if type == 'VARCHAR':
            self.type = '{type}({varchar_length})'.format(type=type, varchar_length=self.varchar_length)
        else:
            self.type = 'INT'
            
    def __str__(self):
        return '`{column_name}` {column_type}'.format(column_name=self.name, column_type=self.type)
    
class ColumnForeignKey(Column):
    def __init__(self, name, db_name, table_name):
        Column.__init__(self, '@'+name.lower())
        self.type = 'INT'
        self.db_name = db_name.lower()
        self.table_name = table_name.lower()
    
    def __str__(self):
        reference_table = self.name[:-2]
        contraint = reference_table+self.name+self.table_name
        
        if reference_table == '':
            return ''
        
        s = ''
        tables = ['athlete', 'club', 'pool', 'meet', 'session', 'event', 'result', 'split', 'swimstyle']
        if self.table_name not in tables or reference_table not in tables:
            s += '-- '
            
        s += ' ALTER TABLE `{db_name}`.`{table_name}`'.format(db_name=self.db_name, table_name=self.table_name)
        s += ' ADD CONSTRAINT `{contraint}`'.format(contraint=contraint)
        s += ' FOREIGN KEY (`{name}`) REFERENCES `{db_name}`.`{reference_table}`'.format(name=self.name, db_name=self.db_name, reference_table=reference_table)        
        s += ' (`{name}`) ON DELETE NO ACTION ON UPDATE NO ACTION;\n'.format(name=self.name)
        
        return s

def main():
    """Read All Files"""
    argv = sys.argv
    path = argv[1] if len(argv) > 2 else (os.popen('pwd').read()[:-1]+'/files')
    file_paths = get_files_path(path)
    folders_path = unzip_files(file_paths)
    xml_to_sql(folders_path)
    
    return 0

if __name__ == '__main__':
    main()
