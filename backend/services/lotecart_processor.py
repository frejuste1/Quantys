import pandas as pd
import logging
from typing import Tuple, List, Dict, Any, Optional
import json

logger = logging.getLogger(__name__)

class LotecartProcessor:
    """
    Service spécialisé pour le traitement des lots LOTECART avec priorisation
    
    LOTECART = Lot d'écart automatique créé quand:
    - Quantité Théorique = 0 (pas de stock prévu)
    - Quantité Réelle > 0 (stock trouvé lors de l'inventaire)
    
    PRIORITÉ: Les LOTECART sont traités EN PREMIER avant tous les autres ajustements
    """
    
    def __init__(self):
        self.lotecart_counter = 0
        self.processed_lotecart = []  # Historique des LOTECART traités
    
    def detect_and_process_lotecart_priority(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]:
        """
        ÉTAPE 1: Détection et traitement prioritaire des LOTECART
        
        Args:
            completed_df: DataFrame du template complété
            original_df: DataFrame des données originales
            
        Returns:
            Tuple (lotecart_candidates, lotecart_adjustments, summary)
        """
        logger.info("🎯 DÉBUT TRAITEMENT PRIORITAIRE LOTECART")
        
        try:
            # 1. Détection des candidats LOTECART
            lotecart_candidates = self.detect_lotecart_candidates(completed_df)
            
            if lotecart_candidates.empty:
                logger.info("ℹ️ Aucun candidat LOTECART détecté")
                return lotecart_candidates, [], self._create_empty_summary()
            
            # 2. Création des ajustements LOTECART avec priorité absolue
            lotecart_adjustments = self.create_priority_lotecart_adjustments(
                lotecart_candidates, original_df
            )
            
            # 3. Validation des ajustements créés
            validation_result = self._validate_lotecart_adjustments(
                lotecart_adjustments, lotecart_candidates
            )
            
            if not validation_result["success"]:
                logger.error(f"❌ Validation LOTECART échouée: {validation_result['issues']}")
                raise ValueError(f"Validation LOTECART échouée: {validation_result['issues']}")
            
            # 4. Génération du résumé
            summary = self.get_lotecart_summary(lotecart_candidates, lotecart_adjustments)
            
            # 5. Marquer les LOTECART comme traités
            self.processed_lotecart = lotecart_adjustments.copy()
            
            logger.info(f"✅ TRAITEMENT PRIORITAIRE LOTECART TERMINÉ: {len(lotecart_adjustments)} ajustements créés")
            
            return lotecart_candidates, lotecart_adjustments, summary
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement prioritaire LOTECART: {e}", exc_info=True)
            raise
    
    def detect_lotecart_candidates(self, completed_df: pd.DataFrame) -> pd.DataFrame:
        """
        Détecte les candidats LOTECART dans le fichier complété
        
        Args:
            completed_df: DataFrame du template complété avec quantités réelles
            
        Returns:
            DataFrame contenant uniquement les candidats LOTECART
        """
        try:
            if completed_df.empty:
                logger.warning("DataFrame complété vide pour détection LOTECART")
                return pd.DataFrame()
            
            # Nettoyer et convertir les colonnes
            df_clean = completed_df.copy()
            
            # Conversion sécurisée des quantités
            df_clean["Quantité Théorique"] = pd.to_numeric(
                df_clean["Quantité Théorique"], errors="coerce"
            ).fillna(0)
            
            df_clean["Quantité Réelle"] = pd.to_numeric(
                df_clean["Quantité Réelle"], errors="coerce"
            ).fillna(0)
            
            # Critère LOTECART: Qté Théorique = 0 ET Qté Réelle > 0
            lotecart_mask = (
                (df_clean["Quantité Théorique"] == 0) & 
                (df_clean["Quantité Réelle"] > 0)
            )
            
            lotecart_candidates = df_clean[lotecart_mask].copy()
            
            if not lotecart_candidates.empty:
                # Marquer comme LOTECART et calculer l'écart
                lotecart_candidates["Type_Lot"] = "lotecart"
                lotecart_candidates["Écart"] = lotecart_candidates["Quantité Réelle"]
                lotecart_candidates["Is_Lotecart"] = True
                lotecart_candidates["Priority"] = 1  # Priorité maximale
                
                logger.info(f"🎯 {len(lotecart_candidates)} candidats LOTECART détectés")
                
                # Log détaillé pour traçabilité
                for _, row in lotecart_candidates.iterrows():
                    logger.info(
                        f"   📦 LOTECART PRIORITAIRE: {row['Code Article']} "
                        f"(Inv: {row.get('Numéro Inventaire', 'N/A')}) - "
                        f"Qté Théo=0 → Qté Réelle={row['Quantité Réelle']}"
                    )
            else:
                logger.info("ℹ️ Aucun candidat LOTECART détecté")
            
            return lotecart_candidates
            
        except Exception as e:
            logger.error(f"❌ Erreur détection candidats LOTECART: {e}", exc_info=True)
            return pd.DataFrame()
    
    def create_priority_lotecart_adjustments(
        self, 
        lotecart_candidates: pd.DataFrame, 
        original_df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Crée les ajustements LOTECART avec priorité absolue
        
        Args:
            lotecart_candidates: DataFrame des candidats LOTECART
            original_df: DataFrame des données originales Sage X3
            
        Returns:
            Liste des ajustements LOTECART prioritaires
        """
        adjustments = []
        
        try:
            if lotecart_candidates.empty:
                logger.info("ℹ️ Aucun candidat LOTECART à traiter")
                return adjustments
            
            logger.info(f"🔥 TRAITEMENT PRIORITAIRE DE {len(lotecart_candidates)} CANDIDATS LOTECART")
            
            for _, candidate in lotecart_candidates.iterrows():
                code_article = candidate["Code Article"]
                numero_inventaire = candidate.get("Numéro Inventaire", "")
                quantite_reelle_saisie = float(candidate["Quantité Réelle"])
                
                # Trouver la ligne de référence dans les données originales
                reference_query = original_df["CODE_ARTICLE"] == code_article
                
                if numero_inventaire:
                    reference_query &= original_df["NUMERO_INVENTAIRE"] == numero_inventaire
                
                reference_lots = original_df[reference_query]
                
                if not reference_lots.empty:
                    # Prendre la première ligne comme référence (ou celle avec quantité = 0)
                    zero_qty_lots = reference_lots[reference_lots["QUANTITE"] == 0]
                    ref_lot = zero_qty_lots.iloc[0] if not zero_qty_lots.empty else reference_lots.iloc[0]
                    
                    # Créer l'ajustement LOTECART PRIORITAIRE
                    adjustment = {
                        "CODE_ARTICLE": code_article,
                        "NUMERO_INVENTAIRE": numero_inventaire,
                        "NUMERO_LOT": "LOTECART",
                        "TYPE_LOT": "lotecart",
                        "PRIORITY": 1,  # Priorité maximale
                        "QUANTITE_ORIGINALE": 0,  # Toujours 0 pour LOTECART
                        "QUANTITE_REELLE_SAISIE": quantite_reelle_saisie,  # Quantité saisie (colonne G)
                        "QUANTITE_CORRIGEE": quantite_reelle_saisie,  # Quantité corrigée (colonne F)
                        "AJUSTEMENT": quantite_reelle_saisie,  # Écart = quantité saisie
                        "Date_Lot": None,  # Pas de date pour LOTECART
                        "original_s_line_raw": ref_lot.get("original_s_line_raw"),  # Ligne de référence
                        "reference_line": ref_lot.get("original_s_line_raw"),
                        "is_new_lotecart": True,  # Flag spécial LOTECART
                        "is_priority_processed": True,  # Flag priorité
                        # Métadonnées pour traçabilité
                        "metadata": {
                            "detection_reason": "qty_theo_0_qty_real_positive",
                            "reference_lot": ref_lot.get("NUMERO_LOT", ""),
                            "reference_site": ref_lot.get("SITE", ""),
                            "reference_emplacement": ref_lot.get("EMPLACEMENT", ""),
                            "processing_priority": "LOTECART_FIRST",
                            "quantite_theo_originale": 0,
                            "quantite_reelle_saisie": quantite_reelle_saisie
                        }
                    }
                    
                    adjustments.append(adjustment)
                    
                    logger.info(
                        f"✅ AJUSTEMENT LOTECART PRIORITAIRE: {code_article} "
                        f"(Qté Saisie={quantite_reelle_saisie}, Qté Corrigée={quantite_reelle_saisie}, "
                        f"Ref={ref_lot.get('NUMERO_LOT', 'N/A')})"
                    )
                else:
                    logger.warning(
                        f"⚠️ Aucune ligne de référence trouvée pour LOTECART PRIORITAIRE: "
                        f"{code_article} dans inventaire {numero_inventaire}"
                    )
            
            logger.info(f"🎯 {len(adjustments)} ajustements LOTECART PRIORITAIRES créés")
            return adjustments
            
        except Exception as e:
            logger.error(f"❌ Erreur création ajustements LOTECART prioritaires: {e}", exc_info=True)
            return []
    
    def generate_priority_lotecart_lines(
        self, 
        lotecart_adjustments: List[Dict[str, Any]], 
        max_line_number: int = 0
    ) -> List[str]:
        """
        Génère les lignes LOTECART prioritaires pour le fichier final
        
        Args:
            lotecart_adjustments: Liste des ajustements LOTECART prioritaires
            max_line_number: Numéro de ligne maximum existant
            
        Returns:
            Liste des nouvelles lignes LOTECART au format Sage X3
        """
        new_lines = []
        
        try:
            if not lotecart_adjustments:
                logger.info("ℹ️ Aucun ajustement LOTECART prioritaire à générer")
                return new_lines
            
            logger.info(f"🔥 GÉNÉRATION DE {len(lotecart_adjustments)} LIGNES LOTECART PRIORITAIRES")
            
            current_line_number = max_line_number
            
            for adjustment in lotecart_adjustments:
                if not adjustment.get("is_new_lotecart", False):
                    continue
                
                reference_line = adjustment.get("reference_line")
                if not reference_line:
                    logger.warning(
                        f"⚠️ Pas de ligne de référence pour LOTECART prioritaire {adjustment['CODE_ARTICLE']}"
                    )
                    continue
                
                # Parser la ligne de référence
                parts = str(reference_line).split(";")
                
                if len(parts) < 15:
                    logger.warning(
                        f"⚠️ Ligne de référence trop courte ({len(parts)} colonnes) "
                        f"pour LOTECART prioritaire {adjustment['CODE_ARTICLE']}"
                    )
                    continue
                
                # Générer un nouveau numéro de ligne unique
                current_line_number += 1000
                self.lotecart_counter += 1
                
                # Construire la nouvelle ligne LOTECART PRIORITAIRE
                new_parts = parts.copy()
                
                # Quantités pour LOTECART prioritaire
                quantite_corrigee = int(adjustment["QUANTITE_CORRIGEE"])
                quantite_saisie = int(adjustment["QUANTITE_REELLE_SAISIE"])
                
                # Modifications spécifiques LOTECART PRIORITAIRE
                new_parts[3] = str(current_line_number)  # RANG - nouveau numéro
                new_parts[5] = str(quantite_corrigee)    # QUANTITE (colonne F - corrigée)
                new_parts[6] = str(quantite_saisie)      # QUANTITE_REELLE_IN_INPUT (colonne G - saisie)
                new_parts[7] = "2"                       # INDICATEUR_COMPTE - toujours 2 pour LOTECART
                new_parts[14] = "LOTECART"               # NUMERO_LOT - identifiant spécial
                
                # Assurer la cohérence des autres champs (garder référence)
                # SITE, EMPLACEMENT, STATUT, UNITE, ZONE_PK restent identiques
                
                new_line = ";".join(new_parts)
                new_lines.append(new_line)
                
                logger.info(
                    f"✅ LIGNE LOTECART PRIORITAIRE générée: {adjustment['CODE_ARTICLE']} "
                    f"(Ligne={current_line_number}, Col F={quantite_corrigee}, Col G={quantite_saisie})"
                )
            
            logger.info(f"🎯 {len(new_lines)} nouvelles lignes LOTECART PRIORITAIRES générées")
            return new_lines
            
        except Exception as e:
            logger.error(f"❌ Erreur génération lignes LOTECART prioritaires: {e}", exc_info=True)
            return []
    
    def update_existing_lotecart_lines(
        self, 
        original_df: pd.DataFrame, 
        completed_df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Met à jour les lignes LOTECART existantes (quantité théorique = 0)
        
        Args:
            original_df: DataFrame des données originales
            completed_df: DataFrame du template complété
            
        Returns:
            Liste des mises à jour pour les lignes existantes
        """
        updates = []
        
        try:
            # Créer un dictionnaire des quantités saisies
            saisies_dict = {}
            for _, row in completed_df.iterrows():
                key = (
                    row["Code Article"], 
                    row.get("Numéro Inventaire", ""), 
                    str(row.get("Numéro Lot", "")).strip()
                )
                saisies_dict[key] = row["Quantité Réelle"]
            
            # Identifier les lignes originales avec quantité théorique = 0
            zero_qty_lines = original_df[original_df["QUANTITE"] == 0].copy()
            
            if zero_qty_lines.empty:
                logger.info("ℹ️ Aucune ligne existante avec quantité théorique = 0")
                return updates
            
            logger.info(f"🔍 Traitement de {len(zero_qty_lines)} lignes existantes avec quantité théorique = 0")
            
            for _, line in zero_qty_lines.iterrows():
                code_article = line["CODE_ARTICLE"]
                numero_inventaire = line.get("NUMERO_INVENTAIRE", "")
                numero_lot = str(line.get("NUMERO_LOT", "")).strip()
                
                key = (code_article, numero_inventaire, numero_lot)
                quantite_saisie = saisies_dict.get(key, 0)
                
                if quantite_saisie > 0:
                    # Cette ligne doit être mise à jour comme LOTECART
                    update = {
                        "CODE_ARTICLE": code_article,
                        "NUMERO_INVENTAIRE": numero_inventaire,
                        "NUMERO_LOT": "LOTECART",  # Forcer LOTECART
                        "TYPE_LOT": "lotecart",
                        "PRIORITY": 1,
                        "QUANTITE_ORIGINALE": 0,
                        "QUANTITE_REELLE_SAISIE": quantite_saisie,
                        "QUANTITE_CORRIGEE": quantite_saisie,  # Pour LOTECART: corrigée = saisie
                        "AJUSTEMENT": quantite_saisie,
                        "original_s_line_raw": line.get("original_s_line_raw"),
                        "is_existing_line_update": True,  # Flag pour ligne existante
                        "is_priority_processed": True,
                        "metadata": {
                            "update_reason": "existing_zero_qty_with_real_qty",
                            "original_lot": numero_lot,
                            "quantite_theo_originale": 0,
                            "quantite_reelle_saisie": quantite_saisie
                        }
                    }
                    
                    updates.append(update)
                    
                    logger.info(
                        f"✅ MISE À JOUR LOTECART: {code_article} "
                        f"(Lot original: '{numero_lot}' → 'LOTECART', Qté saisie: {quantite_saisie})"
                    )
            
            logger.info(f"🎯 {len(updates)} mises à jour LOTECART pour lignes existantes")
            return updates
            
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour lignes LOTECART existantes: {e}", exc_info=True)
            return []
    
    def _validate_lotecart_adjustments(
        self, 
        adjustments: List[Dict[str, Any]], 
        candidates: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Valide que tous les candidats LOTECART ont été traités
        
        Args:
            adjustments: Liste des ajustements créés
            candidates: DataFrame des candidats détectés
            
        Returns:
            Dictionnaire de validation
        """
        validation = {
            "success": False,
            "issues": [],
            "candidates_count": len(candidates),
            "adjustments_count": len(adjustments)
        }
        
        try:
            # Vérifier que chaque candidat a un ajustement
            candidates_articles = set(candidates["Code Article"].tolist())
            adjustments_articles = set(adj["CODE_ARTICLE"] for adj in adjustments)
            
            missing_adjustments = candidates_articles - adjustments_articles
            if missing_adjustments:
                validation["issues"].append(
                    f"Ajustements manquants pour: {', '.join(missing_adjustments)}"
                )
            
            # Vérifier les quantités
            for adjustment in adjustments:
                if adjustment["QUANTITE_CORRIGEE"] <= 0:
                    validation["issues"].append(
                        f"Quantité corrigée invalide pour {adjustment['CODE_ARTICLE']}: {adjustment['QUANTITE_CORRIGEE']}"
                    )
                
                if adjustment["QUANTITE_REELLE_SAISIE"] <= 0:
                    validation["issues"].append(
                        f"Quantité saisie invalide pour {adjustment['CODE_ARTICLE']}: {adjustment['QUANTITE_REELLE_SAISIE']}"
                    )
            
            validation["success"] = len(validation["issues"]) == 0
            
            if validation["success"]:
                logger.info("✅ Validation ajustements LOTECART prioritaires réussie")
            else:
                logger.warning(f"⚠️ Problèmes validation LOTECART: {validation['issues']}")
            
            return validation
            
        except Exception as e:
            logger.error(f"❌ Erreur validation ajustements LOTECART: {e}", exc_info=True)
            validation["issues"].append(f"Erreur de validation: {str(e)}")
            return validation
    
    def apply_lotecart_to_final_file_lines(
        self, 
        original_lines: List[str], 
        lotecart_adjustments: List[Dict[str, Any]],
        lotecart_updates: List[Dict[str, Any]],
        completed_df: pd.DataFrame
    ) -> Tuple[List[str], List[str]]:
        """
        Applique les ajustements LOTECART aux lignes du fichier final
        
        Args:
            original_lines: Lignes originales du fichier
            lotecart_adjustments: Nouveaux ajustements LOTECART
            lotecart_updates: Mises à jour de lignes existantes
            completed_df: DataFrame du template complété
            
        Returns:
            Tuple (lignes_modifiées, nouvelles_lignes_lotecart)
        """
        try:
            logger.info("🔧 APPLICATION DES AJUSTEMENTS LOTECART AU FICHIER FINAL")
            
            # Créer un dictionnaire des quantités saisies
            saisies_dict = {}
            for _, row in completed_df.iterrows():
                key = (
                    row["Code Article"], 
                    row.get("Numéro Inventaire", ""), 
                    str(row.get("Numéro Lot", "")).strip()
                )
                saisies_dict[key] = row["Quantité Réelle"]
            
            # Créer un dictionnaire des mises à jour LOTECART
            updates_dict = {}
            for update in lotecart_updates:
                key = (
                    update["CODE_ARTICLE"], 
                    update["NUMERO_INVENTAIRE"], 
                    update["metadata"]["original_lot"]
                )
                updates_dict[key] = update
            
            # Traiter les lignes originales
            modified_lines = []
            
            for line in original_lines:
                if not line.startswith("S;"):
                    modified_lines.append(line)
                    continue
                
                parts = line.split(";")
                if len(parts) < 15:
                    modified_lines.append(line)
                    continue
                
                # Extraire les informations de la ligne
                code_article = parts[8]
                numero_inventaire = parts[2]
                numero_lot_original = parts[14].strip()
                quantite_theo_originale = float(parts[5]) if parts[5] else 0
                
                key = (code_article, numero_inventaire, numero_lot_original)
                
                # Vérifier s'il y a une mise à jour LOTECART pour cette ligne
                if key in updates_dict:
                    update = updates_dict[key]
                    
                    # Appliquer la mise à jour LOTECART
                    parts[5] = str(int(update["QUANTITE_CORRIGEE"]))     # Colonne F (corrigée)
                    parts[6] = str(int(update["QUANTITE_REELLE_SAISIE"])) # Colonne G (saisie)
                    parts[7] = "2"                                       # Indicateur
                    parts[14] = "LOTECART"                               # Numéro lot
                    
                    logger.info(
                        f"🔄 LIGNE EXISTANTE MISE À JOUR LOTECART: {code_article} "
                        f"(Lot: '{numero_lot_original}' → 'LOTECART', "
                        f"Qté: {quantite_theo_originale} → {update['QUANTITE_CORRIGEE']})"
                    )
                else:
                    # Ligne normale - juste mettre à jour la quantité saisie (colonne G)
                    quantite_saisie = saisies_dict.get(key, 0)
                    parts[6] = str(int(quantite_saisie)) if quantite_saisie > 0 else "0"
                
                modified_lines.append(";".join(parts))
            
            # Générer les nouvelles lignes LOTECART
            new_lotecart_lines = self.generate_priority_lotecart_lines(
                lotecart_adjustments, max_line_number
            )
            
            logger.info(
                f"✅ APPLICATION LOTECART TERMINÉE: "
                f"{len(modified_lines)} lignes traitées, "
                f"{len(new_lotecart_lines)} nouvelles lignes LOTECART"
            )
            
            return modified_lines, new_lotecart_lines
            
        except Exception as e:
            logger.error(f"❌ Erreur application LOTECART au fichier: {e}", exc_info=True)
            return original_lines, []
    
    def validate_final_lotecart_processing(
        self, 
        final_file_path: str, 
        expected_lotecart_count: int
    ) -> Dict[str, Any]:
        """
        Valide que le traitement LOTECART prioritaire s'est bien déroulé
        
        Args:
            final_file_path: Chemin vers le fichier final généré
            expected_lotecart_count: Nombre de LOTECART attendus
            
        Returns:
            Dictionnaire avec les résultats de validation
        """
        validation_result = {
            "success": False,
            "lotecart_lines_found": 0,
            "correct_indicators": 0,
            "correct_quantities": 0,
            "issues": [],
            "details": []
        }
        
        try:
            if not final_file_path or not os.path.exists(final_file_path):
                validation_result["issues"].append("Fichier final non trouvé")
                return validation_result
            
            logger.info(f"🔍 VALIDATION FINALE LOTECART: {final_file_path}")
            
            # Lire et analyser le fichier final
            lotecart_lines = []
            
            with open(final_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line.startswith('S;') and 'LOTECART' in line:
                        parts = line.split(';')
                        if len(parts) >= 15:
                            lotecart_info = {
                                'line_number': line_num,
                                'article': parts[8],
                                'quantite_theo': parts[5],  # Colonne F
                                'quantite_saisie': parts[6], # Colonne G
                                'indicateur': parts[7],
                                'lot': parts[14]
                            }
                            lotecart_lines.append(lotecart_info)
            
            validation_result["lotecart_lines_found"] = len(lotecart_lines)
            
            # Vérifications détaillées
            correct_indicators = 0
            correct_quantities = 0
            
            for line_info in lotecart_lines:
                # Vérifier l'indicateur
                if line_info['indicateur'] == '2':
                    correct_indicators += 1
                else:
                    validation_result["issues"].append(
                        f"Indicateur incorrect ligne {line_info['line_number']}: "
                        f"{line_info['article']} (indicateur={line_info['indicateur']}, attendu=2)"
                    )
                
                # Vérifier les quantités (F et G doivent être identiques pour LOTECART)
                try:
                    qty_f = float(line_info['quantite_theo'])
                    qty_g = float(line_info['quantite_saisie'])
                    
                    if qty_f > 0 and qty_g > 0 and abs(qty_f - qty_g) < 0.001:
                        correct_quantities += 1
                    else:
                        validation_result["issues"].append(
                            f"Quantités incohérentes ligne {line_info['line_number']}: "
                            f"{line_info['article']} (F={qty_f}, G={qty_g})"
                        )
                except ValueError:
                    validation_result["issues"].append(
                        f"Quantités non numériques ligne {line_info['line_number']}: "
                        f"{line_info['article']}"
                    )
                
                # Ajouter aux détails
                validation_result["details"].append({
                    "article": line_info['article'],
                    "line": line_info['line_number'],
                    "qty_f": line_info['quantite_theo'],
                    "qty_g": line_info['quantite_saisie'],
                    "indicator": line_info['indicateur'],
                    "status": "✅" if line_info['indicateur'] == '2' else "❌"
                })
            
            validation_result["correct_indicators"] = correct_indicators
            validation_result["correct_quantities"] = correct_quantities
            
            # Vérifications globales
            if len(lotecart_lines) < expected_lotecart_count:
                validation_result["issues"].append(
                    f"Nombre de lignes LOTECART insuffisant: {len(lotecart_lines)} < {expected_lotecart_count}"
                )
            
            # Succès si toutes les vérifications passent
            validation_result["success"] = (
                len(validation_result["issues"]) == 0 and
                correct_indicators == len(lotecart_lines) and
                correct_quantities == len(lotecart_lines)
            )
            
            if validation_result["success"]:
                logger.info(
                    f"✅ VALIDATION FINALE LOTECART RÉUSSIE: "
                    f"{len(lotecart_lines)} lignes correctes"
                )
            else:
                logger.warning(
                    f"⚠️ VALIDATION FINALE LOTECART AVEC PROBLÈMES: "
                    f"{len(validation_result['issues'])} problème(s) détecté(s)"
                )
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ Erreur validation finale LOTECART: {e}", exc_info=True)
            validation_result["issues"].append(f"Erreur de validation: {str(e)}")
            return validation_result
    
    def get_lotecart_summary(
        self, 
        lotecart_candidates: pd.DataFrame,
        lotecart_adjustments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Génère un résumé détaillé du traitement LOTECART prioritaire
        
        Args:
            lotecart_candidates: DataFrame des candidats détectés
            lotecart_adjustments: Liste des ajustements créés
            
        Returns:
            Dictionnaire avec le résumé détaillé
        """
        try:
            total_quantity = 0
            articles_by_inventory = {}
            priority_stats = {"new_lines": 0, "updated_lines": 0}
            
            if not lotecart_candidates.empty:
                total_quantity = lotecart_candidates["Quantité Réelle"].sum()
                
                # Grouper par inventaire
                for _, row in lotecart_candidates.iterrows():
                    inv = row.get("Numéro Inventaire", "N/A")
                    if inv not in articles_by_inventory:
                        articles_by_inventory[inv] = []
                    
                    articles_by_inventory[inv].append({
                        "article": row["Code Article"],
                        "quantity": row["Quantité Réelle"],
                        "lot_original": row.get("Numéro Lot", "")
                    })
            
            # Analyser les types d'ajustements
            for adj in lotecart_adjustments:
                if adj.get("is_new_lotecart", False):
                    priority_stats["new_lines"] += 1
                elif adj.get("is_existing_line_update", False):
                    priority_stats["updated_lines"] += 1
            
            summary = {
                "candidates_detected": len(lotecart_candidates),
                "adjustments_created": len(lotecart_adjustments),
                "total_quantity": float(total_quantity),
                "inventories_affected": len(articles_by_inventory),
                "articles_by_inventory": articles_by_inventory,
                "priority_stats": priority_stats,
                "processing_timestamp": pd.Timestamp.now().isoformat(),
                "processing_mode": "PRIORITY_FIRST",
                "validation_status": "PENDING"
            }
            
            logger.info(
                f"📊 RÉSUMÉ LOTECART PRIORITAIRE: "
                f"{summary['candidates_detected']} candidats, "
                f"{summary['total_quantity']} unités, "
                f"{priority_stats['new_lines']} nouvelles lignes, "
                f"{priority_stats['updated_lines']} lignes mises à jour"
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Erreur génération résumé LOTECART prioritaire: {e}", exc_info=True)
            return {
                "candidates_detected": 0,
                "adjustments_created": 0,
                "total_quantity": 0,
                "inventories_affected": 0,
                "articles_by_inventory": {},
                "priority_stats": {"new_lines": 0, "updated_lines": 0},
                "error": str(e),
                "processing_mode": "PRIORITY_FIRST"
            }
    
    def _create_empty_summary(self) -> Dict[str, Any]:
        """Crée un résumé vide pour les cas sans LOTECART"""
        return {
            "candidates_detected": 0,
            "adjustments_created": 0,
            "total_quantity": 0,
            "inventories_affected": 0,
            "articles_by_inventory": {},
            "priority_stats": {"new_lines": 0, "updated_lines": 0},
            "processing_timestamp": pd.Timestamp.now().isoformat(),
            "processing_mode": "PRIORITY_FIRST",
            "validation_status": "NO_LOTECART"
        }
    
    def reset_counter(self):
        """Remet à zéro le compteur LOTECART"""
        self.lotecart_counter = 0
        self.processed_lotecart = []
        logger.debug("🔄 Compteur et historique LOTECART remis à zéro")

# Import nécessaire pour la validation finale
import os