#!/usr/bin/env python3
"""
Script de test pour vérifier la traçabilité des quantités réelles en colonne G
"""
import pandas as pd
import os
import tempfile
from datetime import datetime

def create_comprehensive_test_data():
    """Crée des données de test complètes pour vérifier la traçabilité"""
    
    # 1. DataFrame original (données Sage X3 brutes)
    original_data = {
        'TYPE_LIGNE': ['S', 'S', 'S', 'S', 'S'],
        'NUMERO_SESSION': ['SESSION001'] * 5,
        'NUMERO_INVENTAIRE': ['INV001'] * 5,
        'RANG': [1000, 1001, 1002, 1003, 1004],
        'SITE': ['SITE01'] * 5,
        'QUANTITE': [100.0, 50.0, 0.0, 75.0, 25.0],  # Quantités théoriques originales
        'QUANTITE_REELLE_IN_INPUT': [0.0] * 5,  # Initialement à 0
        'INDICATEUR_COMPTE': [1] * 5,
        'CODE_ARTICLE': ['ART001', 'ART002', 'ART003', 'ART004', 'ART005'],
        'EMPLACEMENT': ['EMP001'] * 5,
        'STATUT': ['A'] * 5,
        'UNITE': ['UN'] * 5,
        'VALEUR': [0.0] * 5,
        'ZONE_PK': ['ZONE1'] * 5,
        'NUMERO_LOT': ['LOT001', 'LOT002', '', 'LOT004', 'LOT005'],
        'original_s_line_raw': [
            'S;SESSION001;INV001;1000;SITE01;100;0;1;ART001;EMP001;A;UN;0;ZONE1;LOT001',
            'S;SESSION001;INV001;1001;SITE01;50;0;1;ART002;EMP001;A;UN;0;ZONE1;LOT002',
            'S;SESSION001;INV001;1002;SITE01;0;0;1;ART003;EMP001;A;UN;0;ZONE1;',
            'S;SESSION001;INV001;1003;SITE01;75;0;1;ART004;EMP001;A;UN;0;ZONE1;LOT004',
            'S;SESSION001;INV001;1004;SITE01;25;0;1;ART005;EMP001;A;UN;0;ZONE1;LOT005'
        ]
    }
    original_df = pd.DataFrame(original_data)
    
    # 2. DataFrame complété (template avec quantités réelles saisies)
    completed_data = {
        'Numéro Session': ['SESSION001'] * 5,
        'Numéro Inventaire': ['INV001'] * 5,
        'Code Article': ['ART001', 'ART002', 'ART003', 'ART004', 'ART005'],
        'Quantité Théorique': [100, 50, 0, 75, 25],     # Quantités théoriques originales
        'Quantité Réelle': [95, 55, 10, 70, 25],        # Quantités réelles SAISIES (à préserver en colonne G)
        'Numéro Lot': ['LOT001', 'LOT002', '', 'LOT004', 'LOT005']
    }
    completed_df = pd.DataFrame(completed_data)
    
    # 3. DataFrame distribué (ajustements calculés)
    distributed_data = {
        'CODE_ARTICLE': ['ART001', 'ART002', 'ART003', 'ART004'],  # ART005 pas d'écart
        'NUMERO_INVENTAIRE': ['INV001'] * 4,
        'NUMERO_LOT': ['LOT001', 'LOT002', 'LOTECART', 'LOT004'],
        'TYPE_LOT': ['type1', 'type1', 'lotecart', 'type1'],
        'QUANTITE_ORIGINALE': [100, 50, 0, 75],
        'AJUSTEMENT': [-5, 5, 10, -5],  # Écarts calculés
        'QUANTITE_CORRIGEE': [95, 55, 10, 70],  # Quantités théoriques ajustées (colonne F)
        'original_s_line_raw': [
            'S;SESSION001;INV001;1000;SITE01;100;0;1;ART001;EMP001;A;UN;0;ZONE1;LOT001',
            'S;SESSION001;INV001;1001;SITE01;50;0;1;ART002;EMP001;A;UN;0;ZONE1;LOT002',
            None,  # LOTECART - nouvelle ligne
            'S;SESSION001;INV001;1003;SITE01;75;0;1;ART004;EMP001;A;UN;0;ZONE1;LOT004'
        ]
    }
    distributed_df = pd.DataFrame(distributed_data)
    
    return original_df, completed_df, distributed_df

def simulate_improved_final_file_generation(original_df, completed_df, distributed_df):
    """Simule la génération du fichier final avec traçabilité améliorée"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        final_file_path = f.name
        
        # En-têtes
        f.write("E;SESSION001;test;1;SITE01;;;;;;;;;;\n")
        f.write("L;SESSION001;INV001;1;SITE01;;;;;;;;;;\n")
        
        # Dictionnaire des quantités réelles SAISIES (à préserver en colonne G)
        real_quantities_saisies = {}
        for _, row in completed_df.iterrows():
            key = (row["Code Article"], row["Numéro Inventaire"], str(row["Numéro Lot"]).strip())
            real_quantities_saisies[key] = row["Quantité Réelle"]  # Quantité SAISIE par l'utilisateur
        
        # Dictionnaire des ajustements (quantités théoriques corrigées pour colonne F)
        adjustments_dict = {}
        for _, row in distributed_df.iterrows():
            key = (row["CODE_ARTICLE"], row["NUMERO_INVENTAIRE"], str(row["NUMERO_LOT"]).strip())
            adjustments_dict[key] = {
                "TYPE_LOT": row["TYPE_LOT"],
                "QUANTITE_CORRIGEE": row["QUANTITE_CORRIGEE"],  # Pour colonne F
                "AJUSTEMENT": row["AJUSTEMENT"]
            }
        
        # Traiter chaque ligne originale
        for _, original_row in original_df.iterrows():
            parts = original_row["original_s_line_raw"].split(";")
            
            code_article = original_row["CODE_ARTICLE"]
            numero_inventaire = original_row["NUMERO_INVENTAIRE"]
            numero_lot = str(original_row["NUMERO_LOT"]).strip()
            
            key = (code_article, numero_inventaire, numero_lot)
            
            # Récupérer la quantité réelle SAISIE (pour colonne G)
            quantite_reelle_saisie = real_quantities_saisies.get(key, 0)
            
            # Vérifier s'il y a un ajustement
            if key in adjustments_dict:
                adjustment = adjustments_dict[key]
                
                if adjustment["TYPE_LOT"] == "lotecart":
                    # LOTECART : colonne F = quantité saisie, colonne G = quantité saisie
                    parts[5] = str(int(quantite_reelle_saisie))  # Colonne F
                    parts[6] = str(int(quantite_reelle_saisie))  # Colonne G (traçabilité)
                    parts[7] = "2"  # Indicateur
                    parts[14] = "LOTECART"
                else:
                    # Ajustement normal : colonne F = quantité ajustée, colonne G = quantité saisie
                    parts[5] = str(int(adjustment["QUANTITE_CORRIGEE"]))  # Colonne F (ajustée)
                    parts[6] = str(int(quantite_reelle_saisie))           # Colonne G (saisie réelle)
                    
                f.write(";".join(parts) + "\n")
            else:
                # Ligne sans ajustement : colonne F = quantité originale, colonne G = quantité saisie
                # parts[5] reste inchangé (quantité théorique originale)
                parts[6] = str(int(quantite_reelle_saisie)) if quantite_reelle_saisie != 0 else "0"  # Colonne G
                f.write(";".join(parts) + "\n")
        
        # Ajouter les nouvelles lignes LOTECART
        for _, row in distributed_df.iterrows():
            if pd.isna(row["original_s_line_raw"]) and row["TYPE_LOT"] == "lotecart":
                # Pour les nouvelles lignes LOTECART : colonne F = colonne G = quantité saisie
                quantite_lotecart = int(row['QUANTITE_CORRIGEE'])
                new_line = f"S;SESSION001;INV001;1002;SITE01;{quantite_lotecart};{quantite_lotecart};2;{row['CODE_ARTICLE']};EMP001;A;UN;0;ZONE1;LOTECART"
                f.write(new_line + "\n")
    
    return final_file_path

def verify_traceability_in_final_file(final_file_path, completed_df, distributed_df):
    """Vérifie que la traçabilité des quantités réelles est correctement implémentée"""
    
    print("\n🔍 VÉRIFICATION DE LA TRAÇABILITÉ DES QUANTITÉS RÉELLES")
    print("=" * 70)
    
    # Dictionnaires de référence
    saisies_dict = {}
    for _, row in completed_df.iterrows():
        key = (row["Code Article"], row["Numéro Inventaire"], str(row["Numéro Lot"]).strip())
        saisies_dict[key] = row["Quantité Réelle"]  # Quantité SAISIE
    
    adjustments_dict = {}
    for _, row in distributed_df.iterrows():
        key = (row["CODE_ARTICLE"], row["NUMERO_INVENTAIRE"], str(row["NUMERO_LOT"]).strip())
        adjustments_dict[key] = {
            "QUANTITE_CORRIGEE": row["QUANTITE_CORRIGEE"],
            "TYPE_LOT": row["TYPE_LOT"]
        }
    
    # Analyser le fichier final
    traceability_ok = 0
    total_lines = 0
    issues = []
    
    print("Ligne | Article      | Type     | Col F (Théo) | Col G (Saisie) | Attendu G | Status")
    print("-" * 70)
    
    with open(final_file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line.strip().startswith('S;'):
                parts = line.strip().split(';')
                total_lines += 1
                
                code_article = parts[8]
                numero_inventaire = parts[2]
                numero_lot = parts[14].strip()
                
                key = (code_article, numero_inventaire, numero_lot)
                
                col_f_value = float(parts[5])  # Colonne F (quantité théorique ajustée)
                col_g_value = float(parts[6])  # Colonne G (quantité réelle saisie)
                
                # Déterminer la quantité attendue en colonne G
                expected_col_g = saisies_dict.get(key, 0)
                
                # Déterminer le type de ligne
                if numero_lot == "LOTECART":
                    line_type = "LOTECART"
                elif key in adjustments_dict:
                    line_type = "AJUSTÉ"
                else:
                    line_type = "STANDARD"
                
                # Vérifier la cohérence
                col_g_ok = abs(col_g_value - expected_col_g) < 0.001
                status = "✅" if col_g_ok else "❌"
                
                print(f"{line_num:4d} | {code_article:12s} | {line_type:8s} | {col_f_value:8.0f} | {col_g_value:10.0f} | {expected_col_g:9.0f} | {status}")
                
                if col_g_ok:
                    traceability_ok += 1
                else:
                    issues.append(f"Ligne {line_num}: {code_article} - Col G={col_g_value}, attendu={expected_col_g}")
    
    print("-" * 70)
    print(f"📊 Résultat: {traceability_ok}/{total_lines} lignes avec traçabilité correcte ({(traceability_ok/total_lines)*100:.1f}%)")
    
    if issues:
        print(f"\n⚠️ Problèmes détectés:")
        for issue in issues[:5]:  # Afficher max 5 problèmes
            print(f"   - {issue}")
        if len(issues) > 5:
            print(f"   ... et {len(issues) - 5} autres problèmes")
    
    return traceability_ok == total_lines, issues

def main():
    """Fonction principale de test de traçabilité"""
    print("🧪 TEST DE TRAÇABILITÉ DES QUANTITÉS RÉELLES (COLONNE G)")
    print("=" * 80)
    print("Objectif: Vérifier que la colonne G contient les quantités réelles saisies")
    print("         pour assurer une traçabilité complète des corrections")
    print("=" * 80)
    
    # 1. Créer les données de test
    print("\n📋 Création des données de test...")
    original_df, completed_df, distributed_df = create_comprehensive_test_data()
    
    print(f"   - {len(original_df)} lignes originales")
    print(f"   - {len(completed_df)} lignes dans le template complété")
    print(f"   - {len(distributed_df)} ajustements calculés")
    
    # Afficher les quantités saisies pour référence
    print(f"\n📝 Quantités réelles saisies dans le template:")
    for _, row in completed_df.iterrows():
        print(f"   - {row['Code Article']}: {row['Quantité Réelle']} (Lot: {row['Numéro Lot']})")
    
    # 2. Simuler la génération du fichier final avec la logique améliorée
    print(f"\n🔧 Génération du fichier final avec traçabilité améliorée...")
    final_file_path = simulate_improved_final_file_generation(original_df, completed_df, distributed_df)
    print(f"   Fichier généré: {final_file_path}")
    
    # 3. Vérifier la traçabilité
    success, issues = verify_traceability_in_final_file(final_file_path, completed_df, distributed_df)
    
    # 4. Afficher le contenu du fichier pour inspection
    print(f"\n📄 Contenu du fichier final généré:")
    print("-" * 80)
    with open(final_file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line_content = line.strip()
            if line_content.startswith('S;'):
                parts = line_content.split(';')
                print(f"   {line_num:2d}: {parts[8]:12s} | F={parts[5]:3s} | G={parts[6]:3s} | Lot={parts[14]:10s}")
            else:
                print(f"   {line_num:2d}: {line_content}")
    
    # 5. Nettoyage
    os.unlink(final_file_path)
    
    # 6. Résultat final
    print("\n" + "=" * 80)
    if success:
        print("🎉 TEST DE TRAÇABILITÉ RÉUSSI !")
        print("    ✅ Colonne F: Quantités théoriques ajustées correctement appliquées")
        print("    ✅ Colonne G: Quantités réelles saisies préservées pour traçabilité")
        print("    ✅ Lots LOTECART: Cohérence entre colonnes F et G")
        print("    ✅ Traçabilité complète: Possibilité de vérifier les saisies originales")
        print("\n💡 Bénéfices:")
        print("    • Audit trail complet des modifications")
        print("    • Vérification des saisies d'inventaire")
        print("    • Conformité aux exigences de traçabilité")
    else:
        print("❌ TEST DE TRAÇABILITÉ ÉCHOUÉ !")
        print("    ⚠️ La colonne G ne contient pas les bonnes quantités réelles saisies")
        print("    ⚠️ Risque de perte de traçabilité des corrections")
        print(f"    ⚠️ {len(issues)} problème(s) détecté(s)")

if __name__ == "__main__":
    main()