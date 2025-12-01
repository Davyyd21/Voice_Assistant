import csv
from rapidfuzz import process

# #incarcare comenzi
# def load_commands(csv_path):
#     commands = {}
#     try:
#         with open(csv_path, "r", encoding="utf-8") as f:  #deschizi csv in modul read cu encoding utf-8 cu numele f
#             reader = csv.reader(f) #reader-citeste linie cu linie
#             next(reader, None)  # skip header
#             for row in reader:
#                 if len(row) < 3:
#                     continue #parcurge fiecare linie din csv si daca are mai putin de 3 coloane(e invalid) il sare
#                 key = row[0].strip() #key/nume de comanda
#                 variants = [v.strip().lower() for v in row[1].split("|") if v.strip()] #variatii ale comenzii
#                 action = row[2].strip() #comanda
#                 commands[key] = {
#                     "variants": variants,
#                     "action": action
#                 } #definire dictionar
#         print(f"[LOADED] {len(commands)} commands from {csv_path}")
#     except Exception as e:
#         print(f"[ERROR CSV] {e}")
#     return commands

#incarcare comenzi
def load_commands(csv_path):
    # variant -> command_key
    var2key = {}
    # command_key -> action (shell command or description)
    key2act = {}
    variants_vec = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:  #deschizi csv in modul read cu encoding utf-8 cu numele f
            reader = csv.reader(f) #reader-citeste linie cu linie
            next(reader, None)  # skip header
            for row in reader:
                if len(row) < 3:
                    continue #parcurge fiecare linie din csv si daca are mai putin de 3 coloane(e invalid) il sare
                key = row[0].strip() #key/nume de comanda
                variants = [v.strip().lower() for v in row[1].split("|") if v.strip()] #variatii ale comenzii
                variants_vec.extend(variants)
                action = row[2].strip() #comanda (local action / description)
                key2act[key] = action
                for variant in variants:
                    var2key[variant] = key

        print(f"[LOADED] {len(variants_vec)} variants from {csv_path}")
    except Exception as e:
        print(f"[ERROR CSV] {e}")
    return var2key, key2act, variants_vec

#gasire cel mai bun match din dictionar
def find_best_match(input_text, var2key, key2act, var_vect, cutoff=70):
    input_text = input_text.lower().strip()

    # Use score_cutoff for safety
    result = process.extractOne(input_text, var_vect, score_cutoff=cutoff)
    if result is None:
        return None

    best_variant, score, _ = result  # unpack safely
    command_key = var2key.get(best_variant)
    action = key2act.get(command_key)
    return command_key, action, score
