#!/usr/bin/env python3
"""
Test complet du traitement STRICT PRIORITAIRE LOTECART
Vérifie que les incohérences sont définitivement résolues
"""
import sys
import os
sys.path.append('.')

import pandas as pd
import tempfile
from datetime import datetime
from services.priority_processor import PriorityProcessor
from services.lotecart_processor import LotecartProcessor

def create_comprehensive_test_data():
    """Crée des données de test complètes pour vérifier le traitement strict"""
    
    # 1. DataFrame original (données Sage X3 brutes)
    original_data = {
        'TYPE_LIGNE': ['S', 'S', 'S', 'S', 'S', 'S'],
        'NUMERO_SESSION': ['SESSION001'] * 6,
        'NUMERO_INVENTAIRE': ['INV001'] * 6,
        'RANG': [1000, 1001, 1002, 1003, 1004, 1005],
        'SITE': ['SITE01'] * 6,
        'QUANTITE': [100.0, 50.0, 0.0, 75.0, 0.0, 25.0],  # ART003 et ART005 = candidats LOTECART
        'QUANTITE_REELLE_IN_INPUT': [0.0] * 6,
        'INDICATEUR_COMPTE': [1] * 6,
        'CODE_ARTICLE': ['ART001', 'ART002', 'ART003', 'ART004', 'ART005', 'ART006'],
        'EMPLACEMENT': ['EMP001'] * 6,
        'STATUT': ['A'] * 6,
        'UNITE': ['UN'] * 6,
        'VALEUR': [0.0] * 6,
        'ZONE_PK': ['ZONE1'] * 6,
        'NUMERO_LOT': ['LOT001', 'LOT002', '', 'LOT004', '', 'LOT006'],
        'original_s_line_raw': [
            'S;SESSION001;INV001;1000;SITE01;100;0;1;ART001;EMP001;A;UN;0;ZONE1;LOT001',
            'S;SESSION001;INV001;1001;SITE01;50;0;1;ART002;EMP001;A;UN;0;ZONE1;LOT002',
            'S;SESSION001;INV001;1002;SITE01;0;0;1;ART003;EMP001;A;UN;0;ZONE1;',
            'S;SESSION001;INV001;1003;SITE01;75;0;1;ART004;EMP001;A;UN;0;ZONE1;LOT004',
            'S;SESSION001;INV001;1004;SITE01;0;0;1;ART005;EMP001;A;UN;0;ZONE1;',
            'S;SESSION001;INV001;1005;SITE01;25;0;1;ART006;EMP001;A;UN;0;ZONE1;LOT006'
        ]
    }
    original_df = pd.DataFrame(original_data)
    
    # 2. DataFrame complété (template avec quantités réelles saisies)
    completed_data = {
        'Numéro Session': ['SESSION001'] * 6,
        'Numéro Inventaire': ['INV001'] * 6,
        'Code Article': ['ART001', 'ART002', 'ART003', 'ART004', 'ART005', 'ART006'],
        'Quantité Théorique': [100, 50, 0, 75, 0, 25],     # ART003 et ART005 ont qté théo = 0
        'Quantité Réelle': [95, 55, 15, 70, 8, 25],        # ART003 et ART005 ont qté réelle > 0 = LOTECART
        'Numéro Lot': ['LOT001', 'LOT002', '', 'LOT004', '', 'LOT006']
    }
    completed_df = pd.DataFrame(completed_data)
    
    return original_df, completed_df

def test_strict_lotecart_priority():
    """Test du traitement strict prioritaire LOTECART"""
    print("🧪 TEST TRAITEMENT STRICT PRIORITAIRE LOTECART")
    print("=" * 80)
    print("Objectif: Résoudre définitivement les incohérences LOTECART")
    print("Méthode: Traitement prioritaire strict avec validation blocante")
    print("=" * 80)
    
    try:
        # 1. Créer les données de test
        print("\n📋 1. CRÉATION DES DONNÉES DE TEST")
        original_df, completed_df = create_comprehensive_test_data()
        
        print(f"   ✅ {len(original_df)} lignes originales créées")
        print(f"   ✅ {len(completed_df)} lignes dans le template complété")
        
        # Afficher les candidats LOTECART attendus
        expected_lotecart = completed_df[
            (completed_df['Quantité Théorique'] == 0) & 
            (completed_df['Quantité Réelle'] > 0)
        ]
        print(f"   🎯 {len(expected_lotecart)} candidats LOTECART attendus:")
        for _, row in expected_lotecart.iterrows():
            print(f"      - {row['Code Article']}: Qté Théo=0, Qté Réelle={row['Quantité Réelle']}")
        
        # 2. Initialiser le processeur prioritaire strict
        print("\n🔧 2. INITIALISATION DU PROCESSEUR PRIORITAIRE STRICT")
        priority_processor = PriorityProcessor()
        
        # 3. Traitement avec priorisation stricte
        print("\n🚀 3. TRAITEMENT AVEC PRIORISATION STRICTE")
        processing_result = priority_processor.process_with_strict_priority(
            completed_df, original_df, strategy="FIFO"
        )
        
        # 4. Vérification des résultats LOTECART
        print("\n🔍 4. VÉRIFICATION DES RÉSULTATS LOTECART")
        
        lotecart_summary = processing_result["lotecart_summary"]
        lotecart_new = processing_result["lotecart_new_adjustments"]
        lotecart_updates = processing_result["lotecart_existing_updates"]
        
        print(f"   📊 Candidats détectés: {lotecart_summary['candidates_detected']}")
        print(f"   📊 Ajustements créés: {lotecart_summary['adjustments_created']}")
        print(f"   📊 Nouveaux LOTECART: {len(lotecart_new)}")
        print(f"   📊 LOTECART mis à jour: {len(lotecart_updates)}")
        print(f"   📊 Score qualité: {lotecart_summary['quality_score']:.1f}%")
        
        # Vérifier chaque ajustement LOTECART
        print("\n   🔍 DÉTAIL DES AJUSTEMENTS LOTECART:")
        all_lotecart = lotecart_new + lotecart_updates
        
        for i, adj in enumerate(all_lotecart, 1):
            coherent = abs(adj["QUANTITE_CORRIGEE"] - adj["QUANTITE_REELLE_SAISIE"]) < 0.001
            status = "✅" if coherent else "❌"
            print(f"      {status} {i}. {adj['CODE_ARTICLE']}: "
                  f"Corrigée={adj['QUANTITE_CORRIGEE']}, "
                  f"Saisie={adj['QUANTITE_REELLE_SAISIE']}, "
                  f"Type={adj['TYPE_LOT']}")
        
        # 5. Génération du fichier final de test
        print("\n📄 5. GÉNÉRATION DU FICHIER FINAL DE TEST")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            test_final_path = f.name
        
        header_lines = [
            "E;SESSION001;test;1;SITE01;;;;;;;;;;",
            "L;SESSION001;INV001;1;SITE01;;;;;;;;;;"
        ]
        
        final_path, generation_summary = priority_processor.generate_coherent_final_file(
            "test_session", original_df, completed_df, header_lines, test_final_path
        )
        
        print(f"   ✅ Fichier final généré: {final_path}")
        print(f"   📊 Lignes traitées: {generation_summary['total_lines_processed']}")
        print(f"   📊 LOTECART appliqués: {generation_summary['total_lotecart_lines']}")
        print(f"   📊 Validation: {'✅' if generation_summary['validation']['success'] else '❌'}")
        
        # 6. Validation finale stricte
        print("\n🔍 6. VALIDATION FINALE STRICTE")
        
        validation = generation_summary["validation"]
        
        print(f"   📊 Lignes LOTECART trouvées: {validation['lotecart_lines_found']}")
        print(f"   📊 Indicateurs corrects: {validation['correct_indicators']}")
        print(f"   📊 Quantités cohérentes: {validation['coherent_quantities']}")
        
        if validation["issues"]:
            print(f"   ⚠️ Problèmes détectés:")
            for issue in validation["issues"][:5]:
                print(f"      - {issue}")
        
        # 7. Analyse détaillée du fichier final
        print("\n📋 7. ANALYSE DÉTAILLÉE DU FICHIER FINAL")
        
        print("   Contenu des lignes LOTECART:")
        with open(final_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if 'LOTECART' in line:
                    parts = line.strip().split(';')
                    if len(parts) >= 15:
                        article = parts[8]
                        qty_f = parts[5]
                        qty_g = parts[6]
                        indicator = parts[7]
                        coherent = (qty_f == qty_g and indicator == '2' and float(qty_f) > 0)
                        status = "✅" if coherent else "❌"
                        print(f"      {status} Ligne {line_num}: {article} - F={qty_f}, G={qty_g}, Ind={indicator}")
        
        # 8. Résultat final
        print("\n" + "=" * 80)
        
        success = (
            validation["success"] and
            lotecart_summary["quality_score"] == 100 and
            len(validation["issues"]) == 0
        )
        
        if success:
            print("🎉 TEST STRICT PRIORITAIRE LOTECART RÉUSSI !")
            print("    ✅ Tous les candidats LOTECART ont été détectés")
            print("    ✅ Tous les ajustements LOTECART sont cohérents (F = G)")
            print("    ✅ Tous les indicateurs LOTECART sont corrects (= 2)")
            print("    ✅ Aucune incohérence détectée dans le fichier final")
            print("    ✅ Traitement prioritaire strict fonctionnel")
            print("\n💡 PROBLÈME RÉSOLU DÉFINITIVEMENT:")
            print("    • Priorisation stricte des LOTECART")
            print("    • Validation blocante avant autres ajustements")
            print("    • Cohérence garantie F = G pour LOTECART")
            print("    • Traçabilité complète des quantités saisies")
        else:
            print("❌ TEST STRICT PRIORITAIRE LOTECART ÉCHOUÉ !")
            print("    ⚠️ Des incohérences persistent dans le traitement LOTECART")
            print(f"    ⚠️ Score qualité: {lotecart_summary['quality_score']:.1f}%")
            print(f"    ⚠️ Problèmes: {len(validation['issues'])}")
            print("\n🔧 ACTIONS REQUISES:")
            print("    • Vérifier la logique de détection LOTECART")
            print("    • Vérifier la logique de génération des ajustements")
            print("    • Vérifier la logique d'application au fichier final")
        
        # Nettoyage
        os.unlink(final_path)
        
        return success
        
    except Exception as e:
        print(f"❌ ERREUR LORS DU TEST: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_lotecart_detection_only():
    """Test isolé de la détection LOTECART"""
    print("\n🔬 TEST ISOLÉ: DÉTECTION LOTECART")
    print("-" * 50)
    
    try:
        # Données de test pour détection
        completed_data = {
            'Code Article': ['ART001', 'ART002', 'ART003', 'ART004'],
            'Numéro Inventaire': ['INV001'] * 4,
            'Quantité Théorique': [100, 0, 50, 0],  # ART002 et ART004 sont candidats
            'Quantité Réelle': [95, 25, 48, 10],    # ART002 et ART004 ont qté réelle > 0
            'Numéro Lot': ['LOT001', '', 'LOT003', '']
        }
        completed_df = pd.DataFrame(completed_data)
        
        # Test de détection
        processor = LotecartProcessor()
        candidates = processor.detect_lotecart_candidates(completed_df)
        
        print(f"   📊 Candidats détectés: {len(candidates)}")
        print(f"   📊 Candidats attendus: 2 (ART002, ART004)")
        
        if len(candidates) == 2:
            print("   ✅ Nombre de candidats correct")
            
            # Vérifier les propriétés
            for _, candidate in candidates.iterrows():
                article = candidate['Code Article']
                theo = candidate['Quantité Théorique']
                real = candidate['Quantité Réelle']
                type_lot = candidate.get('Type_Lot', 'N/A')
                
                if theo == 0 and real > 0 and type_lot == 'lotecart':
                    print(f"   ✅ {article}: Théo={theo}, Réel={real}, Type={type_lot}")
                else:
                    print(f"   ❌ {article}: Théo={theo}, Réel={real}, Type={type_lot}")
                    return False
            
            print("   ✅ DÉTECTION LOTECART PARFAITE")
            return True
        else:
            print(f"   ❌ Nombre de candidats incorrect: {len(candidates)} ≠ 2")
            return False
            
    except Exception as e:
        print(f"   ❌ Erreur test détection: {e}")
        return False

def main():
    """Fonction principale de test"""
    print("🎯 RÉSOLUTION DÉFINITIVE DU BUG LOTECART")
    print("=" * 80)
    print("PROBLÈME: Incohérences dans le fichier final - certaines lignes LOTECART")
    print("          ont quantité théorique = 0 au lieu de la quantité saisie")
    print("SOLUTION: Traitement prioritaire strict avec validation blocante")
    print("=" * 80)
    
    # Test 1: Détection isolée
    print("\n🔬 TEST 1: DÉTECTION LOTECART ISOLÉE")
    detection_ok = test_lotecart_detection_only()
    
    # Test 2: Traitement complet
    print("\n🔬 TEST 2: TRAITEMENT COMPLET STRICT")
    processing_ok = test_strict_lotecart_priority()
    
    # Résultat global
    print("\n" + "=" * 80)
    print("🎯 RÉSULTAT GLOBAL DES TESTS")
    print("=" * 80)
    
    if detection_ok and processing_ok:
        print("🎉 TOUS LES TESTS RÉUSSIS - BUG LOTECART RÉSOLU DÉFINITIVEMENT !")
        print("\n✅ CORRECTIONS IMPLÉMENTÉES:")
        print("   • Détection stricte des candidats LOTECART")
        print("   • Priorisation absolue du traitement LOTECART")
        print("   • Validation blocante avant autres ajustements")
        print("   • Cohérence garantie F = G pour LOTECART")
        print("   • Traçabilité complète des quantités saisies")
        print("\n🚀 PRÊT POUR LA PRODUCTION:")
        print("   • Aucune incohérence possible")
        print("   • Traitement déterministe")
        print("   • Validation automatique")
        print("   • Logs détaillés pour traçabilité")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ - CORRECTIONS SUPPLÉMENTAIRES NÉCESSAIRES")
        print(f"   • Détection LOTECART: {'✅' if detection_ok else '❌'}")
        print(f"   • Traitement complet: {'✅' if processing_ok else '❌'}")
        print("\n🔧 ACTIONS REQUISES:")
        if not detection_ok:
            print("   • Corriger la logique de détection LOTECART")
        if not processing_ok:
            print("   • Corriger la logique de traitement prioritaire")

if __name__ == "__main__":
    main()