import psycopg2

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='Rosa',
                              database='infint_integrated')
cursor = connection.cursor()

cursor.execute('ALTER TABLE movie ALTER COLUMN process_used_to_produce TYPE character varying(255)')

cursor.execute('SELECT id,process_used_to_produce FROM movie WHERE id LIKE \'uci_%\'')
for row in cursor.fetchall():
	movie_id = row[0]
	process_used_to_produce = row[1]
	#parse process_used_to_produce 
	if process_used_to_produce is not None:
		if ';' in process_used_to_produce:
			processes = process_used_to_produce.split(';')
		elif ' ' in process_used_to_produce:
			processes = process_used_to_produce.split(' ')
		elif '\\' in process_used_to_produce:
			processes = process_used_to_produce.split('\\')
		else:
			processes = process_used_to_produce.split(';')
		process_used_to_produce = ''
		for element in processes:
			process = ''
			element = element.strip()
			if '\\' in element:
				element = element.replace('\\', '')
			if element == 'bnw':
				process += 'Black and White'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Tcol' or element == 'Tcol?':
				process += 'Technicolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'col':
				process += 'Color'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'bws' or element == 'sbw':
				process += 'Black and White Silent'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Ancol' or element == 'Ascol':
				process += 'Anscocolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'CS':
				process += 'Cinemascope'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'cld':
				process += 'recolored black and white'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Pan':
				process += 'PanaVision'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'VsVis':
				process += 'PanaVision'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Ecol':
				process += 'Eastmancolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'DeLuxe':
				process += 'DeLuxe'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Mcol':
				process += 'Metrocolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Wcol':
				process += 'Warnercolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Trama':
				process += 'Technirama'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Mlab':
				process += 'Movielab'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'Pathecol':
				process += 'Pathecolor'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == 'TV':
				process += 'film made for TV'
				if element != processes[len(processes) - 1]:
					process += '; '
			elif element == '' or element == 'prc' or element == '$' or element == 'b-0w' or element == 'TS' or element == 'AO' or element == 'Cof':
				if process.endswith(';')
					process = process[:-2]
				pass
			else:
				process += element
				if element != processes[len(processes) - 1]:
					process += '; '
			if process != '':
				process_used_to_produce += process
		cursor.execute('UPDATE movie SET process_used_to_produce = %s WHERE id = %s', [process_used_to_produce, movie_id])
	connection.commit()

