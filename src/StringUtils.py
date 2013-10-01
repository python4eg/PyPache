import re

def get_args(recv_data):
    get_pat = re.compile('GET\s([\w./%-]+)+(\?((\w+)=([\w%.]+)&?)+)?')
    http_pat = re.compile('(HTTP/[.0-9]+)')
    host_pat = re.compile('Host:\s([\w.:]+)')
    agent_part = re.compile('User-Agent:\s(.+)\r')
    get = get_pat.search(recv_data).group(1)
    args = get_pat.search(recv_data).group(2)
    http = http_pat.search(recv_data).group(1)
    host = host_pat.search(recv_data).group(1)
    agent = agent_part.search(recv_data).group(1)
    dict_args = {}
    if args is not None:
        args = args[1:].split('&')
        for i in args:
            splt = i.find('=')
            dict_args.update({i[:splt]:i[splt+1:]})
    ret_dict = {
                'path' : get,
                'http' : http,
                'args' : dict_args,
                'host' : host,
                'agent' : agent
                }
    return ret_dict


def urlstoc(get_path):
    file_path = get_path
    symbols = ''
    while True:
        pos = file_path.find('%')
        if pos >= 0:
            symbol = file_path[pos:pos+3]
            file_path = file_path[:pos] + file_path[pos+3:]
            symbols += symbol
        else:
            break

    cnt = 0
    while cnt < len(symbols):
        ad = 0
        if symbols[cnt:cnt+3] == '%D0':
            get_path = get_path.replace(symbols[cnt:cnt+3], '')
            cnt += 3
            ad = -16
        elif symbols[cnt:cnt+3] == '%D1':
            get_path = get_path.replace(symbols[cnt:cnt+3], '')
            cnt += 3
            ad = 96
        g = symbols[cnt:cnt+3].replace('%', '0x').lower()
        get_path = get_path.replace(symbols[cnt:cnt+3], chr(int(str(g), 16) + ad))
        cnt += 3
    return get_path

def put_args(data, args, fsize):
    pass

def size_to_maxbyte(size):
    named = ['B', 'Kb', 'Mb', 'Gb', 'Tb']
    count = 0
    size = float(size)
    for i in range(5):
        if size / 1024 > 1:
            size /= 1024
            count += 1
    size = ("%%.%if" % 2) % size
    n_size = '%s %s' % (size, named[count])
    return n_size


def put_tag(data, pos, tag, fsize):
    server_tag = '<center>' + str(tag) + '</center>'
    data = data[:pos] + server_tag + data[pos:]
    fsize += len(server_tag)
    return data, fsize


def type_split(get_path):
    type = re.compile('[\w/-]\.(\w+)$')
    ret_type = type.search(get_path)
    if ret_type is None:
        ret_type = ''
    else:
        ret_type = '.%s' % ret_type.group(1)
    return ret_type
