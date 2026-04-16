import csv
import json
import os

# --- Configuration ---
CSV_PATH = os.path.join('scraper', 'output', 'UNT_Alumni_Data.csv')
JSON_PATH = os.path.join('scraper', 'data', 'companies.json')

def load_json(path):
    if not os.path.exists(path):
        print(f"❌ Error: JSON file not found at {path}")
        return {"companies": [], "universities": [], "job_titles": [], "aliases": {}}
    
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved updates to {path}")

def clean(text):
    if not text: return ""
    return text.strip()

def ask_user(item, context, options):
    """
    Generic function to ask the user what to do with a missing item.
    """
    print(f"\n⚠️  MISSING: '{item}' (Found in {context})")
    print(f"   It is not in your companies.json.")
    
    for i, opt in enumerate(options, 1):
        print(f"   [{i}] {opt}")
    print(f"   [s] Skip (Don't add)")
    
    while True:
        choice = input("   👉 Choice: ").lower().strip()
        if choice == 's': return None
        
        # Check if valid number
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx] # Return the label of the list to add to (e.g. "companies")
        
        print("   ❌ Invalid choice. Try again.")

def main():
    data = load_json(JSON_PATH)
    
    # Convert lists to sets for faster checking (we will convert back before saving)
    companies_set = set(data.get("companies", []))
    universities_set = set(data.get("universities", []))
    job_titles_set = set(data.get("job_titles", []))
    
    # Helper to check aliases
    aliases = data.get("aliases", {})
    def is_known(name, collection):
        return (name in collection) or (name in aliases)

    # Track if we modified anything
    modified_json = False
    modified_csv = False

    try:
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader) # Read all to memory so we can enumerate safely

        print(f"🔍 Scanning {len(rows)} rows against companies.json...\n")

        for i, row in enumerate(rows, start=2):
            name = row.get('name', 'Unknown')
            profile_url = row.get('profile_url', 'N/A')

            # --- 0. Validate Job Info Completeness ---
            checks = [
                ('job_title', 'company', ['job_start_date', 'job_end_date'], 'Current Job'),
                ('exp2_title', 'exp2_company', ['exp2_dates'], 'Exp 2'),
                ('exp3_title', 'exp3_company', ['exp3_dates'], 'Exp 3')
            ]

            for t_col, c_col, d_cols, context in checks:
                t_val = clean(row.get(t_col))
                c_val = clean(row.get(c_col))
                d_has_val = any(clean(row.get(d)) for d in d_cols)

                # If any field present, title AND company must be present
                if t_val or c_val or d_has_val:
                    missing = []
                    if not t_val:
                        missing.append(t_col)
                    if not c_val:
                        missing.append(c_col)
                    
                    if missing:
                        print(f"\n⚠️  Warning: {name} ({context}) has incomplete job info.")
                        print(f"   Present: Title='{t_val}', Company='{c_val}', Dates Present={d_has_val}")
                        print(f"   Missing: {', '.join(missing)}")
                        
                        # Interactive fix
                        do_fix = input("   Enter missing info? (y/n/1 to skip): ").lower().strip()
                        if do_fix == 'y':
                            for field in missing:
                                new_val = input(f"   Enter value for '{field}': ").strip()
                                row[field] = new_val
                                modified_csv = True

            # --- 1. Validate Job Titles ---
            for col, context in [('job_title', 'Current Job'), ('exp2_title', 'Exp 2'), ('exp3_title', 'Exp 3')]:
                val = clean(row.get(col))
                if val and val not in job_titles_set:
                    action = ask_user(val, f"{name} - {context}", ["Add to 'job_titles'"])
                    if action == "Add to 'job_titles'":
                        job_titles_set.add(val)
                        data["job_titles"].append(val)
                        modified_json = True
                        print(f"   ✅ Added '{val}' to job_titles.")

            # --- 2. Validate Education ---
            edu_val = clean(row.get('education'))
            if edu_val:
                if not (is_known(edu_val, universities_set) or is_known(edu_val, companies_set)):
                    action = ask_user(edu_val, f"{name} - Education", ["Add to 'universities'", "Add to 'companies'"])
                    if action == "Add to 'universities'":
                        universities_set.add(edu_val)
                        data["universities"].append(edu_val)
                        modified_json = True
                        print(f"   ✅ Added '{edu_val}' to universities.")
                    elif action == "Add to 'companies'":
                        companies_set.add(edu_val)
                        data["companies"].append(edu_val)
                        modified_json = True
                        print(f"   ✅ Added '{edu_val}' to companies.")

            # --- 3. Validate Companies ---
            for col, context in [('company', 'Current Company'), ('exp2_company', 'Exp 2 Company'), ('exp3_company', 'Exp 3 Company')]:
                val = clean(row.get(col))
                if val:
                    if not (is_known(val, companies_set) or is_known(val, universities_set)):
                        action = ask_user(val, f"{name} - {context}", ["Add to 'companies'", "Add to 'universities'"])
                        if action == "Add to 'companies'":
                            companies_set.add(val)
                            data["companies"].append(val)
                            modified_json = True
                            print(f"   ✅ Added '{val}' to companies.")
                        elif action == "Add to 'universities'":
                            universities_set.add(val)
                            data["universities"].append(val)
                            modified_json = True
                            print(f"   ✅ Added '{val}' to universities.")

    except KeyboardInterrupt:
        print("\n\n🛑 Script cancelled by user.")
        if modified_json or modified_csv:
            save = input("Save changes made so far? (y/n): ").lower()
            if save == 'y':
                if modified_json: save_json(data, JSON_PATH)
                if modified_csv:
                    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(rows)
                    print(f"💾 Saved updates to {CSV_PATH}")
        return

    print("\n" + "="*50)
    if modified_json:
        save_json(data, JSON_PATH)
    if modified_csv:
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"💾 Saved updates to {CSV_PATH}")
        print("🎉 Validation Complete. Files Updated.")
    elif not modified_json:
        print("🎉 Validation Complete. No changes needed.")

if __name__ == "__main__":
    main()
