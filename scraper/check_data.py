import csv
import json
import os

# --- Configuration ---
CSV_PATH = 'scraper/output/UNT_Alumni_Data.csv'
JSON_PATH = r"C:\Users\Sangam Bartaula\Documents\GitHub\alumni-networking-tool\scraper\data\companies.json"

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
    modified = False

    try:
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader) # Read all to memory so we can enumerate safely

        print(f"🔍 Scanning {len(rows)} rows against companies.json...\n")

        for i, row in enumerate(rows, start=2):
            name = row.get('name', 'Unknown')
            
            # --- 1. Validate Job Titles ---
            # Checks job_title, exp2_title, exp3_title
            title_cols = [('job_title', 'Current Job'), ('exp2_title', 'Exp 2'), ('exp3_title', 'Exp 3')]
            
            for col, context in title_cols:
                val = clean(row.get(col))
                if val and val not in job_titles_set:
                    # Logic: If job title not in job_titles, ask to add or skip
                    action = ask_user(val, f"{name} - {context}", ["Add to 'job_titles'"])
                    
                    if action == "Add to 'job_titles'":
                        job_titles_set.add(val)
                        data["job_titles"].append(val)
                        modified = True
                        print(f"   ✅ Added '{val}' to job_titles.")

            # --- 2. Validate Education ---
            # "if education from csv is not in companies or education, print error and ask..."
            edu_val = clean(row.get('education'))
            if edu_val:
                # Check if it exists in EITHER universities OR companies
                if not (is_known(edu_val, universities_set) or is_known(edu_val, companies_set)):
                    action = ask_user(edu_val, f"{name} - Education", [
                        "Add to 'universities'", 
                        "Add to 'companies'"
                    ])
                    
                    if action == "Add to 'universities'":
                        universities_set.add(edu_val)
                        data["universities"].append(edu_val)
                        modified = True
                        print(f"   ✅ Added '{edu_val}' to universities.")
                    elif action == "Add to 'companies'":
                        companies_set.add(edu_val)
                        data["companies"].append(edu_val)
                        modified = True
                        print(f"   ✅ Added '{edu_val}' to companies.")

            # --- 3. Validate Companies ---
            # Checks company, exp2_company, exp3_company
            comp_cols = [('company', 'Current Company'), ('exp2_company', 'Exp 2 Company'), ('exp3_company', 'Exp 3 Company')]
            
            for col, context in comp_cols:
                val = clean(row.get(col))
                if val:
                    # Check against companies AND universities (since people work at universities)
                    if not (is_known(val, companies_set) or is_known(val, universities_set)):
                        action = ask_user(val, f"{name} - {context}", [
                            "Add to 'companies'", 
                            "Add to 'universities'"
                        ])
                        
                        if action == "Add to 'companies'":
                            companies_set.add(val)
                            data["companies"].append(val)
                            modified = True
                            print(f"   ✅ Added '{val}' to companies.")
                        elif action == "Add to 'universities'":
                            universities_set.add(val)
                            data["universities"].append(val)
                            modified = True
                            print(f"   ✅ Added '{val}' to universities.")

    except KeyboardInterrupt:
        print("\n\n🛑 Script cancelled by user.")
        if modified:
            save = input("Save changes made so far? (y/n): ").lower()
            if save == 'y':
                save_json(data, JSON_PATH)
        return

    print("\n" + "="*50)
    if modified:
        save_json(data, JSON_PATH)
        print("🎉 Validation Complete. JSON Updated.")
    else:
        print("🎉 Validation Complete. No changes needed.")

if __name__ == "__main__":
    main()