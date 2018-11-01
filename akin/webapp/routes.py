from webapp import webapp, akin
from flask import render_template, request, flash, redirect, url_for, jsonify
from werkzeug.utils import secure_filename
import os
import csv
import json

@webapp.route('/')
@webapp.route('/index')
def index():
    return render_template('home.html', akin=akin)

@webapp.route('/delete_datasource')
def delete_dataset():
    dataset_to_delete = request.args.get('datasource')
    success, result = akin.delete_datasource(dataset_to_delete)
    return result

@webapp.route('/get_datasets')
def get_datasets():
    datasets = jsonify([dsk for dsk, dkv in akin.datasources.items()])
    return datasets

@webapp.route('/get_groups/<dsname>')
def get_groups(dsname):
    groups = jsonify([])
    datasource = akin.datasources.get(dsname)
    if datasource:
        groups = jsonify([g.field for g in datasource.groups.values()])
    return groups

@webapp.route('/get_fields/<dsname>')
def get_fields(dsname):
    fields = jsonify([])
    datasource = akin.datasources.get(dsname)
    if datasource and datasource.data:
        fields = jsonify([h for h in datasource.data[0].keys() if not h.startswith('__')])
    return fields

@webapp.route('/create_group')
def create_group():
    dsname = request.args.get('dataset')
    field_name = request.args.get('field')
    group_template = request.args.get('group_template')

    datasource = akin.datasources.get(dsname)
    group_settings = akin.grouptemplates.get(group_template)
    if datasource and datasource.data and group_settings:
        akin.group_data(datasource, field_name, group_settings)
    return jsonify({'success':True})

@webapp.route('/get_group_data')
def get_group_data():    
    dsname = request.args.get('dataset')
    group_name = request.args.get('group')
    data_entries = list()
    headers = list()
    datasource = akin.datasources.get(dsname)
    if datasource:
        group = datasource.groups.get(group_name)
        if group:
            #h.encode('ascii', 'ignore').decode('ascii')
            headers = [h for h in datasource.data[0].keys()]
            # Establish ids for the groups for display purposes
            group_id = 0
            for gl in group.values:
                group_id += 1
                for g in gl:
                    g['group_id'] = group_id
            data_entries = [item for sublist in group.values for item in sublist]
            headers = [h for h in data_entries[0].keys() if not h.startswith('_')]
            data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_')] for de in data_entries]
            #data_entries = [['<br/>'.join(map(str, i)) for i in zip(*[[gv for gk, gv in g.items()] for g in item])] for item in group.values]
            # headers = [h for h in datasource.data[0].keys() if not h.startswith('_')]
            # data_entries = [['<br/>'.join(map(str, i)) for i in zip(*[[gv for gk, gv in g.items() if not gk.startswith('_')] for g in item])] for item in group.values]
            #['\r\n'.join(map(str, i)) for i in zip(*[g.values() for g in group.values[0]])]
            #data_entries = [item for sublist in group.values for item in sublist]
            #data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_')] for de in data_entries]
            #data_entries = [g.values for g in group]
            #data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_')] for de in data_entries]
            return json.dumps({'headers': headers, 'data': data_entries})

@webapp.route('/dataviewer', methods = ['POST'])
def dataviewer():
    dsname = request.form.get('dataset')
    group_name = request.form.get('group')
    data_entries = list()
    headers = list()
    datasource = akin.datasources.get(dsname)
    if datasource:
        group = datasource.groups.get(group_name)
        if group:
            headers = [h for h in datasource.data[0].keys() if not h.startswith('_')]
            data_entries = [['\r\n'.join(map(str, i)) for i in zip(*[g.values() for g in item])] for item in group.values]
            # headers = [h for h in datasource.data[0].keys() if not h.startswith('_')]
            # data_entries = [['\r\n'.join(map(str, i)) for i in zip(*[g.values() for g in item])] for item in group.values]
            #data_entries = [item for sublist in group.values for item in sublist]
            #data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_')] for de in data_entries]
            #data_entries = [g.values for g in group]
            #data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_')] for de in data_entries]
            return render_template('dataviewer.html', data_headers=headers, data_entires=data_entries)

@webapp.route('/asyncupload', methods = ['GET', 'POST'])
def asyncupload():
    f = request.files['file']
    filename = secure_filename(f.filename)
    filepath = os.path.join(webapp.config['UPLOAD_FOLDER'], filename)
    f.save(filepath)

    all_data_entries = []
    with open(filepath, 'r', encoding='utf8') as data_file:
        for row in csv.DictReader(data_file, dialect="excel"):
            all_data_entries.append(row)

    success, result = akin.add_datasource(filename, all_data_entries)
    return result

@webapp.route('/uploadfile', methods = ['GET', 'POST'])
def uploadfile():
    if request.method == 'POST':
        f = request.files['file']
        filename = secure_filename(f.filename)
        filepath = os.path.join(webapp.config['UPLOAD_FOLDER'], filename)
        f.save(filepath)

        all_data_entries = []
        with open(filepath, 'r', encoding='utf8') as data_file:
            for row in csv.DictReader(data_file, dialect="excel"):
                all_data_entries.append(row)

        akin.add_datasource(filename, all_data_entries)

        return 'file uploaded successfully'
