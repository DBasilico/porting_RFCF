from datetime import datetime, timedelta
import calendar as cl
import holidays
from dateutil.relativedelta import relativedelta

def split_dateformat(s):
    if s.find('stringa') > -1:
        return s.split('_#_')
    else:
        return [s, None]

def IIF(cond, yes, no):
    if cond:
        out = yes
    else:
        out = no
    return out

def ROUNDWEB(num, prc):
    return num

def MONTH(date):
    fm = "%Y-%m-%d %H:%M:%S"
    if type(date) == str:
        tmp = datetime.strptime(date, fm)
    elif type(date) == int:
        tmp = datetime.fromtimestamp(date)
    else:
        tmp = datetime.fromtimestamp(int(date))

    return tmp.month

def YEAR(date):
    fm = "%Y-%m-%d %H:%M:%S"
    if type(date) == str:
        tmp = datetime.strptime(date, fm)
    elif type(date) == int:
        tmp = datetime.fromtimestamp(date)
    else:
        tmp = datetime.fromtimestamp(int(date))

    return tmp.year

def DATESERIAL(y, m, d):
    fm = "%Y-%m-%d %H:%M:%S"
    tmp = datetime(int(y), int(m), int(d))
    return tmp

def DATEDIFF(no, data1, data2):
    return abs((data2 - data1).days)

def ev(str, arr, v):
    value_var = arr

    i = 0
    vett = []
    while i < len(value_var):
        if ((v[i] != 'CONTRACTTYPE') & (v[i] != 'CODCONTRACTTYPE') & (v[i] != 'CCATYPE') & (v[i] != 'HEC')):
            vett.append(float(value_var[i]))
        else:
            vett.append(value_var[i])
        i += 1

    value_var = vett

    out = eval(str)
    return out


def var_extract(arr):
    array = arr
    var = []
    j = 0
    for i in range(1, len(arr), 2):
        var.append([arr[i], j])
        j += 1
    return var


def var_replace(arr):
    array = arr
    # var=[]
    null = ''
    j = 0
    for i in range(1, len(arr), 2):
        # var.append(arr[i])
        array[i] = 'value_var[' + str(j) + ']'
        j += 1
    array = null.join(array)
    return array

def comp(dateS, dateE, NUM_M):
    """Metodo per dividere l'intervallo di competenza in base al numero di mesi.

    Input:  dateS       --> colonna data inizio competenza
            dateE       --> colonna data fine competenza
            NUM_M       --> colonna intervallo mensile di competenza

    Output: StartDate   --> colonna data inizio competenza mensile
            EndDate     --> colonna data fine competenza mensile
            """

    StartDate = []
    EndDate = []
    for i in range(NUM_M):
        if i == 0:
            if (dateS.month == dateE.month and dateS.year == dateE.year):
                StartDate.append(dateS)
                EndDate.append(dateE)
            else:
                StartDate.append(dateS)
                dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
                EndDate.append(dateS)
                dateS = dateS + timedelta(1)
        elif i == NUM_M - 1:
            StartDate.append(dateS)
            EndDate.append(dateE)
        else:
            StartDate.append(dateS)
            dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
            EndDate.append(dateS)
            dateS = dateS + timedelta(1)
    return StartDate, EndDate

def average_days(lista):
    num = len(lista)
    tot = timedelta(0)
    for i in range(num):
        if i is not num - 1:
            tot = tot + abs(lista[i + 1] - lista[i])
    res = tot / (num - 1)
    return res

def expected_date(date, avg_days, data_emissione, EXPECTED_TYPE):
    expected_dates = [datetime(2018, 12, 1, 0, 0)]
    while True:
        date = date + avg_days
        if (EXPECTED_TYPE == 'AUTOLETTURA'):
            if (date.year < 2021 & data_emissione - date <= timedelta(7) & data_emissione - date >= timedelta(0)):
                expected_dates.append(date)
            elif (date.year > 2020):
                break
        if (EXPECTED_TYPE == 'LETTURA'):
            if (date.year < 2021 & data_emissione - date <= timedelta(6) & data_emissione - date >= timedelta(3)):
                expected_dates.append(date)
            elif (date.year > 2020):
                break
    return max(expected_dates)

def date_competenza(anno, mese, data_inizio_competenza):
    """Metodo per popolare la data_inizio_competenza quando nulla
    Input:  anno    --> colonna anno_comp
            mese    --> colonna mese_comp
            data    --> colonna data_inizio_competenza
    Output: data    --> data al primo mese se nulla
            """
    if (data_inizio_competenza is None):
        data_inizio_competenza = datetime(anno, mese, 1)
    return data_inizio_competenza

def intervalli_fatt(commodity, cadenza, gruppo, lista):
    """Metodo che calcola gli intervalli di fatturazione data l'emissione.

    Input:  commodity     --> colonna commodity
            cadenza       --> colonna cadenza
            gruppo        --> colonna gruppo
            lista         --> colonna con lista date emissione

    Output: start_fattura --> colonna inizio periodo di fatturazione
            end_fattura   --> colonna fine periodo di fatturazione
            date_fattura  --> colonna data emissione
            """
    fm = "%d/%m/%Y"
    start_fattura = []
    end_fattura = []
    date_fattura = []
    lista = [x for x in lista if x != '']

    if ((commodity == 'luce' and cadenza == 'mensile') or gruppo == 'XE'):
        for i in lista:
            date_fattura.append(datetime.strptime(i, fm))
            dateE = datetime.strptime(i, fm)
            dateE = dateE.replace(day=1) - timedelta(1)
            end_fattura.append(dateE)
            dateS = dateE.replace(day=1)
            start_fattura.append(dateS)
    else:
        for i in lista:
            date_fattura.append(datetime.strptime(i, fm))
            dateE = datetime.strptime(i, fm)
            if i == lista[0]:
                dateS = datetime.strptime('01/01/2020', fm)
                start_fattura.append(dateS)
            else:
                dateS = end_fattura[-1]
                dateS = dateS + timedelta(1)
                start_fattura.append(dateS)
            end_fattura.append(dateE)

    return start_fattura, end_fattura, date_fattura

def tempo_to_calendario(anno, mese):
    data_inizio_comp = datetime(anno, mese, 1)
    return data_inizio_comp

def check_scadenza(data):
    cond = 1
    while cond == 1:
        if (data.weekday() == 6 or data in holidays.IT()):
            data = data + timedelta(days=1)
        else:
            cond = 0
    return data


def check_scadenza_lista(data, lista):
    cond = 1
    while cond == 1:
        if (data.weekday() == 6 or data in lista):
            data = data + timedelta(days=1)
        else:
            cond = 0
    return data


def average_days(lista):
    num = len(lista)
    tot = timedelta(0)
    for i in range(num):
        if i is not num - 1:
            tot = tot + abs(lista[i + 1] - lista[i])
    if num - 1 == 0:
        res = tot / 2
    else:
        res = tot / (num - 1)
    return res.days


def aggiunta_mese(data, numero_mese):
    data = data + relativedelta(months=numero_mese)
    return data