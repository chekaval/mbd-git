"""
Title: GitHub Project - Script to Parse Cities from CSV

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
"""
import csv
import re

import json

country_cities_dict = {}


def parse_cities_from_csv():
    with open('worldcitiespop.csv', 'r') as csv_file:
        data = csv.reader(csv_file)
        prefix_set = {""}
        freq = {}

        for row in data:
            cur_city = row[1].split()
            if len(cur_city) > 1:
                if cur_city[0] in freq:
                    freq[cur_city[0]] += 1
                else:
                    freq[cur_city[0]] = 1
                prefix_set.add(cur_city[0].lower().encode("utf-8").lower())
            if row[4] != '':
                try:
                    if float(row[4]) > 150000.0:
                        country = row[0]
                        city = row[1]
                        if country in country_cities_dict:
                            if len(country_cities_dict[country]) != 0:
                                cur_cities = country_cities_dict[country]
                                cur_cities.append(city)
                                country_cities_dict[country] = cur_cities
                        else:
                            country_cities_dict[country] = [city]
                except ValueError:
                    pass
        freq_sort = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        result_set = {''}
        for i in range(50):
            result_set.add(freq_sort[i][0])


def write_to_file():
    with open('cities.json', 'w') as f:
        json.dump(country_cities_dict, f)


def load_cities():
    with open('cities.json') as f:
        return json.load(f)
