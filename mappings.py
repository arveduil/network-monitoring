IP_INFO = {
    'facebook': {
        'range': ('31.13.64.0', '31.13.127.255'),
    },
    'google': {
        'range': ('142.250.0.0', '142.251.255.255')
    },
    'youtube': {
        'range': ('172.217.0.0', '172.217.255.255')
    },
    'amazon': {
        'range': ('176.32.96.0', '176.32.103.255')
    },
    'github': {
        'range': ('140.82.112.0', '140.82.127.255')
    }

}


def convert_ipv4(ip):
    return tuple(int(n) for n in ip.split('.'))


def ipv4_in(addr, ip_range):
    start = ip_range[0]
    end = ip_range[1]
    return convert_ipv4(start) < convert_ipv4(addr) < convert_ipv4(end)


def extract_tag(ip_address):
    for tag, info in IP_INFO.items():
        ip_range = info.get('range')
        single_ip = info.get('single')

        if single_ip and (single_ip == ip_address):
            return tag

        if ip_range and ipv4_in(ip_address, ip_range):
            return tag

