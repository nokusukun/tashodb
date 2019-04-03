import time
import secrets
import random
import matplotlib.pyplot as plt
import pprint

from tinydb import TinyDB, Query
import tasho

def printTime(start, end):
    print(f"Execution Time: {end-start:.4f}s")
    return float(f'{end-start:.4f}')

def testTinyDBInsertSingle(database, testData):

    for i in testData:
        database.insert(i)

    return database


def testTashoDBInsertSingle(table, testData):
    timePlot = []
    for index, data in enumerate(testData):
        start = time.time()
        table.insert(index, data)
        end = time.time()
        timePlot.append(float(f'{end-start:.4f}'))

    return timePlot


def testTinyDBInsert(database, testData):
    database.insert_multiple(testData)
    # for i in testData:
    #     database.insert(i)

    return database


def testTashoDBInsert(table, testData):
    table.bulk_insert({secrets.token_hex(4):data for data in testData})
    return table


def testTinyDBQuery(database, key):
    Key = Query()
    return database.search(Key.keyA == key)


def testTashoDBQuery(database, key):
    return database.query_one(lambda id, Key: Key['keyA'] == key)


def testTashoSolo(chunk_size):
    t = tasho.Database.new(f'TestDataTasho{chunk_size}', chunk_size=chunk_size, open_instead=True).table.TestTable
    tasho.Console.logLevel = 5
    testData = [{
        'keyA': secrets.token_hex(2), 
        'keyB': secrets.token_urlsafe(),
        'keyC': secrets.token_urlsafe(), 
        'keyD': secrets.token_urlsafe(),
        'keyE': secrets.token_urlsafe(), 
        'keyF': secrets.token_urlsafe(),
        'keyG': secrets.token_urlsafe(),} for x in range(100000)]
    testPickups = [random.choice(testData)['keyA'] for x in range(500)]

    t_s = time.time()
    timePlot = testTashoDBInsertSingle(t, testData[0:50000])
    t_e = time.time()
    print(f'Testing TashoDB {chunk_size} Single Insert 50K Items: ', end='')
    t_t = printTime(t_s, t_e)
    pprint.pprint(t.chunks)

    # plt.plot(timePlot)
    # plt.title('TashoDB Single Insert 500K Items')
    # plt.ylabel('Time Required')
    # plt.xlabel('Records')
    # plt.show()

    t_s = time.time()
    testTashoDBInsert(t,testData[50000:])
    t_e = time.time()
    print(f'Testing TashoDB {chunk_size} Bulk Insert 50K Items: ', end='')
    t_t = printTime(t_s, t_e)
    pprint.pprint(t.chunks)
    
    timePlot = []
    d_s = time.time()
    for testPickup in testPickups:
        start = time.time()
        testTashoDBQuery(t, testPickup)
        end = time.time()
        timePlot.append(float(f'{end-start:.4f}'))
    
    d_e = time.time()
    print(f'Testing TashoDB {chunk_size} 500 Queries: ', end='')
    d_t = printTime(d_s, d_e)

    # plt.plot(timePlot)
    # plt.title('TashoDB 500 Queries')
    # plt.ylabel('Time Required')
    # plt.xlabel('Iteration')
    # plt.show()
    t.commit()

def testSuite():
    d = TinyDB('TestDataTiny')
    t = tasho.Database.new('TestDataTasho').table.TestTable
    print('Preparing 50k Test Items')
    testData = [{'keyA': secrets.token_hex(), 'keyB': secrets.token_urlsafe()} for x in range(50000)]
    testPickups = [random.choice(testData)['keyA'] for x in range(500)]


    print('Testing TashoDB Single Insert 1k Items: ', end='')
    t_s = time.time()
    testTashoDBInsertSingle(t,testData[0:1000])
    t_e = time.time()
    t_t = printTime(t_s, t_e)

    print('Testing TinyDB Single Insert 1k Items: ', end='')
    d_s = time.time()
    testTinyDBInsertSingle(d,testData[0:1000])
    d_e = time.time()
    d_t = printTime(d_s, d_e)
    print(f'\nTashoDB Difference: {(d_t/t_t * 100):.4f}%')
    print('---')

    print('Testing TashoDB Single Insert 2k Items: ', end='')
    t_s = time.time()
    testTashoDBInsertSingle(t,testData[1000:3000])
    t_e = time.time()
    t_t = printTime(t_s, t_e)

    print('Testing TinyDB Single Insert 2k Items: ', end='')
    d_s = time.time()
    testTinyDBInsertSingle(d,testData[1000:3000])
    d_e = time.time()
    d_t = printTime(d_s, d_e)
    print(f'\nTashoDB Difference: {(d_t/t_t * 100):.4f}%')
    print('---')

    print('Testing TashoDB Single Insert 5k Items: ', end='')
    t_s = time.time()
    testTashoDBInsertSingle(t,testData[3000:8000])
    t_e = time.time()
    t_t = printTime(t_s, t_e)

    print('Testing TinyDB Single Insert 5k Items: ', end='')
    d_s = time.time()
    testTinyDBInsertSingle(d,testData[3000:8000])
    d_e = time.time()
    d_t = printTime(d_s, d_e)
    print(f'\nTashoDB Difference: {(d_t/t_t * 100):.4f}%')
    print('---')

    print('Testing TashoDB Bulk Insert 42k Items: ', end='')
    t_s = time.time()
    testTashoDBInsert(t, testData[8000:])
    t_e = time.time()
    t_t = printTime(t_s, t_e)

    print('Testing TinyDB Bulk Insert 42k Items: ', end='')
    d_s = time.time()
    testTinyDBInsert(d, testData[8000:])
    d_e = time.time()
    d_t = printTime(d_s, d_e)

    print(f'\nTashoDB Difference: {(d_t/t_t * 100):.4f}%')
    print('---')
    
    print('Testing TashoDB 500 Queries: ', end='')
    t_s = time.time()
    for testPickup in testPickups:
        testTashoDBQuery(t, testPickup)
    t_e = time.time()
    t_t = printTime(t_s, t_e)

    print('Testing TinyDB 500 Queries: ', end='')
    d_s = time.time()
    for testPickup in testPickups:
        testTinyDBQuery(d, testPickup)
    d_e = time.time()
    d_t = printTime(d_s, d_e)

    print(f'\nTashoDB Difference: {(d_t/t_t * 100):.4f}%')
    print('---')

if __name__ == '__main__':
    #testTashoSolo(8192)
    testTashoSolo(16384)
    #testTashoSolo(32768)
    #testTashoSolo(65536)
    #testSuite()