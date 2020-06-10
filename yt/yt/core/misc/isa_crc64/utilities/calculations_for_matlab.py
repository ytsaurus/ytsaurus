import argparse


#poly = "42F0E1EBA9EA3693"
poly = "E543279765927881"
#poly = "ad93d23594c935a9"

def hex_to_bin(h):
    h = int(h, 16)
    h += 2 ** 64
    return str(bin(h))[2:]

def bin_to_hex(b):
    return "0x" + "{:016x}".format(int(b, 2))

def get_power_representation(i): 
    return "".join(list(map(str, [1] + [0] * (64 * i))))

def form_query_variable(var, val):
    return "{0} = gf([{1}])".format(var, ", ".join(val))

def form_query(i):
    assert i >= 1 and i <= 17

    query = form_query_variable("a", get_power_representation(i))
    query += "\n"
    query += form_query_variable("b", hex_to_bin(poly))
    query += "\n"
    query += "[q,r] = deconv(a,b)"
    return query

def parse_answer():
    f = open("answer_of_gf_matlab", "r")
    
    numeric = ""
    for line in f.readlines():
        line = line.strip()
        if not line:
            continue        
        if line[0] != "0" and line[0] != "1":
            continue
        line = line.split(" ")
        line = "".join([l for l in line if l == "0" or l == "1"])
        numeric += line
    num = bin_to_hex(numeric)

    print num
    print (len(num) - 2)

    f.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--get-query-for-power", type=int, default=None)
    parser.add_argument("--parse-answer", action="store_true")
    
    args = parser.parse_args()

    if args.get_query_for_power:
        print form_query(args.get_query_for_power)

    if args.parse_answer:
        parse_answer()
