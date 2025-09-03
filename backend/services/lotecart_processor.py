import pandas as pd
import logging
from typing import Tuple, List, Dict, Any, Optional
import json

logger = logging.getLogger(__name__)

class LotecartProcessor:
    """
    Service spécialisé pour le traitement des lots LOTECART avec logique stricte
    
    LOTECART = Lot d'écart automatique créé quand:
    - Quantité Théorique = 0 (pas de stock prévu)
    - Quantité Réelle > 0 (stock trouvé lors de l'inventaire)
    
    RÈGLES STRICTES:
    - Traitement en priorité absolue
    - Quantité corrigée = Quantité saisie
    - Indicateur toujours = 2
    - Numéro lot = "LOTECART"
    """
    
    def __init__(self):
        self.lotecart_counter = 0
        self.processed_lotecart = []
    
    def detect_lotecart_candidates(self, completed_df: pd.DataFrame) -> pd.DataFrame:
        """
        Détecte les candidats LOTECART avec validation stricte
        
        Args:
            completed_df: DataFrame du template complété avec quantités réelles
            
        Returns:
            DataFrame contenant uniquement les candidats LOTECART validés
        """
        try:
            if completed_df.empty:
                logger.warning("DataFrame complété vide pour détection LOTECART")
                return pd.DataFrame()
            
            logger.info("🔍 DÉTECTION STRICTE DES CANDIDATS LOTECART")
            
            # Nettoyer et valider les données
            df_clean = completed_df.copy()
            
            # Validation et conversion des colonnes critiques
            required_columns = ["Code Article", "Quantité Théorique", "Quantité Réelle"]
            missing_columns = [col for col in required_columns if col not in df_clean.columns]
            
            if missing_columns:
                raise ValueError(f"Colonnes manquantes pour détection LOTECART: {missing_columns}")
            
            # Conversion sécurisée des quantités avec validation
            df_clean["Quantité Théorique"] = pd.to_numeric(
                df_clean["Quantité Théorique"], errors="coerce"
            )
            df_clean["Quantité Réelle"] = pd.to_numeric(
                df_clean["Quantité Réelle"], errors="coerce"
            )
            
            # Vérifier les conversions
            invalid_theo = df_clean["Quantité Théorique"].isna().sum()
            invalid_real = df_clean["Quantité Réelle"].isna().sum()
            
            if invalid_theo > 0 or invalid_real > 0:
                logger.warning(
                    f"⚠️ Quantités invalides détectées: {invalid_theo} théoriques, {invalid_real} réelles"
                )
                # Remplacer les NaN par 0 pour continuer
                df_clean["Quantité Théorique"] = df_clean["Quantité Théorique"].fillna(0)
                df_clean["Quantité Réelle"] = df_clean["Quantité Réelle"].fillna(0)
            
            # CRITÈRE STRICT LOTECART: Qté Théorique = 0 ET Qté Réelle > 0
            lotecart_mask = (
                (df_clean["Quantité Théorique"] == 0) & 
                (df_clean["Quantité Réelle"] > 0)
            )
            
            lotecart_candidates = df_clean[lotecart_mask].copy()
            
            if not lotecart_candidates.empty:
                # Enrichir les candidats avec métadonnées LOTECART
                lotecart_candidates["Type_Lot"] = "lotecart"
                lotecart_candidates["Écart"] = lotecart_candidates["Quantité Réelle"]
                lotecart_candidates["Is_Lotecart"] = True
                lotecart_candidates["Priority"] = 1  # Priorité maximale
                lotecart_candidates["Detection_Timestamp"] = pd.Timestamp.now()
                
                # Validation supplémentaire des candidats
                for _, candidate in lotecart_candidates.iterrows():
                    if candidate["Quantité Réelle"] <= 0:
                        logger.error(
                            f"❌ CANDIDAT LOTECART INVALIDE: {candidate['Code Article']} "
                            f"- Quantité réelle <= 0: {candidate['Quantité Réelle']}"
                        )
                        raise ValueError(f"Candidat LOTECART invalide: {candidate['Code Article']}")
                
                logger.info(f"🎯 {len(lotecart_candidates)} candidats LOTECART VALIDÉS détectés")
                
                # Log détaillé pour traçabilité complète
                for _, row in lotecart_candidates.iterrows():
                    logger.info(
                        f"   📦 CANDIDAT LOTECART VALIDÉ: {row['Code Article']} "
                        f"(Inv: {row.get('Numéro Inventaire', 'N/A')}) - "
                        f"Qté Théo=0 → Qté Réelle={row['Quantité Réelle']} "
                        f"(Lot original: '{row.get('Numéro Lot', '')}')"
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
        Crée les ajustements LOTECART avec logique stricte et priorité absolue
        """
        adjustments = []
        
        try:
            if lotecart_candidates.empty:
                logger.info("ℹ️ Aucun candidat LOTECART à traiter")
                return adjustments
            
            logger.info(f"🔥 CRÉATION AJUSTEMENTS LOTECART PRIORITAIRES: {len(lotecart_candidates)} candidats")
            
            for _, candidate in lotecart_candidates.iterrows():
                code_article = candidate["Code Article"]
                numero_inventaire = candidate.get("Numéro Inventaire", "")
                quantite_reelle_saisie = float(candidate["Quantité Réelle"])
                numero_lot_original = str(candidate.get("Numéro Lot", "")).strip()
                
                # Validation stricte du candidat
                if quantite_reelle_saisie <= 0:
                    logger.error(f"❌ CANDIDAT INVALIDE: {code_article} - Quantité saisie <= 0")
                    continue
                
                # Trouver la ligne de référence dans les données originales
                reference_query = (
                    (original_df["CODE_ARTICLE"] == code_article) &
                    (original_df["NUMERO_INVENTAIRE"] == numero_inventaire)
                )
                
                # Si le candidat a un lot original, chercher cette ligne spécifique
                if numero_lot_original:
                    specific_query = reference_query & (
                        original_df["NUMERO_LOT"].astype(str).str.strip() == numero_lot_original
                    )
                    specific_lots = original_df[specific_query]
                    
                    if not specific_lots.empty:
                        reference_lots = specific_lots
                    else:
                        # Fallback sur toutes les lignes de l'article
                        reference_lots = original_df[reference_query]
                else:
                    reference_lots = original_df[reference_query]
                
                if reference_lots.empty:
                    logger.error(
                        f"❌ AUCUNE LIGNE DE RÉFÉRENCE pour LOTECART: {code_article} "
                        f"(Inv: {numero_inventaire}, Lot original: '{numero_lot_original}')"
                    )
                    continue
                
                # Prendre la ligne avec quantité = 0 en priorité, sinon la première
                zero_qty_lots = reference_lots[reference_lots["QUANTITE"] == 0]
                ref_lot = zero_qty_lots.iloc[0] if not zero_qty_lots.empty else reference_lots.iloc[0]
                
                # Créer l'ajustement LOTECART avec logique stricte
                adjustment = {
                    "CODE_ARTICLE": code_article,
                    "NUMERO_INVENTAIRE": numero_inventaire,
                    "NUMERO_LOT": "LOTECART",  # Toujours LOTECART
                    "TYPE_LOT": "lotecart",
                    "PRIORITY": 1,  # Priorité maximale
                    "QUANTITE_ORIGINALE": 0,  # Toujours 0 pour LOTECART
                    "QUANTITE_REELLE_SAISIE": quantite_reelle_saisie,  # Quantité saisie (colonne G)
                    "QUANTITE_CORRIGEE": quantite_reelle_saisie,       # Quantité corrigée = saisie (colonne F)
                    "AJUSTEMENT": quantite_reelle_saisie,              # Écart = quantité saisie
                    "Date_Lot": None,  # Pas de date pour LOTECART
                    "original_s_line_raw": ref_lot.get("original_s_line_raw"),
                    "reference_line": ref_lot.get("original_s_line_raw"),
                    "is_new_lotecart": True,  # Flag nouveau LOTECART
                    "is_priority_processed": True,  # Flag priorité
                    "is_coherent": True,  # Flag cohérence
                    # Métadonnées complètes pour traçabilité
                    "metadata": {
                        "detection_reason": "qty_theo_0_qty_real_positive",
                        "original_lot": numero_lot_original,
                        "reference_site": ref_lot.get("SITE", ""),
                        "reference_emplacement": ref_lot.get("EMPLACEMENT", ""),
                        "reference_zone": ref_lot.get("ZONE_PK", ""),
                        "processing_priority": "LOTECART_ABSOLUTE_FIRST",
                        "quantite_theo_originale": 0,
                        "quantite_reelle_saisie": quantite_reelle_saisie,
                        "coherence_rule": "F_EQUALS_G_FOR_LOTECART",
                        "validation_timestamp": pd.Timestamp.now().isoformat()
                    }
                }
                
                adjustments.append(adjustment)
                
                logger.info(
                    f"✅ AJUSTEMENT LOTECART PRIORITAIRE CRÉÉ: {code_article} "
                    f"(Qté Saisie={quantite_reelle_saisie}, Qté Corrigée={quantite_reelle_saisie}, "
                    f"Lot original: '{numero_lot_original}' → 'LOTECART')"
                )
            
            logger.info(f"🎯 {len(adjustments)} ajustements LOTECART PRIORITAIRES créés avec succès")
            return adjustments
            
        except Exception as e:
            logger.error(f"❌ Erreur création ajustements LOTECART prioritaires: {e}", exc_info=True)
            return []
    
    def update_existing_lotecart_lines(
        self, 
        original_df: pd.DataFrame, 
        completed_df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Met à jour les lignes existantes avec quantité théorique = 0 et quantité réelle > 0
        """
        updates = []
        
        try:
            logger.info("🔄 MISE À JOUR DES LIGNES EXISTANTES LOTECART")
            
            # Créer un dictionnaire des quantités saisies
            saisies_dict = {}
            for _, row in completed_df.iterrows():
                key = (
                    row["Code Article"], 
                    row.get("Numéro Inventaire", ""), 
                    str(row.get("Numéro Lot", "")).strip()
                )
                saisies_dict[key] = float(row["Quantité Réelle"])
            
            # Identifier les lignes originales avec quantité théorique = 0
            zero_qty_lines = original_df[original_df["QUANTITE"] == 0].copy()
            
            if zero_qty_lines.empty:
                logger.info("ℹ️ Aucune ligne existante avec quantité théorique = 0")
                return updates
            
            logger.info(f"🔍 Analyse de {len(zero_qty_lines)} lignes existantes avec quantité théorique = 0")
            
            for _, line in zero_qty_lines.iterrows():
                code_article = line["CODE_ARTICLE"]
                numero_inventaire = line.get("NUMERO_INVENTAIRE", "")
                numero_lot_original = str(line.get("NUMERO_LOT", "")).strip()
                
                key = (code_article, numero_inventaire, numero_lot_original)
                quantite_saisie = saisies_dict.get(key, 0)
                
                # Vérifier si cette ligne doit devenir LOTECART
                if quantite_saisie > 0:
                    # Validation stricte
                    if quantite_saisie <= 0:
                        logger.error(f"❌ QUANTITÉ SAISIE INVALIDE pour {code_article}: {quantite_saisie}")
                        continue
                    
                    # Créer la mise à jour LOTECART
                    update = {
                        "CODE_ARTICLE": code_article,
                        "NUMERO_INVENTAIRE": numero_inventaire,
                        "NUMERO_LOT": "LOTECART",  # Forcer LOTECART
                        "TYPE_LOT": "lotecart",
                        "PRIORITY": 1,  # Priorité maximale
                        "QUANTITE_ORIGINALE": 0,  # Toujours 0
                        "QUANTITE_REELLE_SAISIE": quantite_saisie,  # Quantité saisie (colonne G)
                        "QUANTITE_CORRIGEE": quantite_saisie,       # Quantité corrigée = saisie (colonne F)
                        "AJUSTEMENT": quantite_saisie,              # Écart = quantité saisie
                        "Date_Lot": line.get("Date_Lot"),
                        "original_s_line_raw": line.get("original_s_line_raw"),
                        "is_existing_line_update": True,  # Flag ligne existante
                        "is_priority_processed": True,    # Flag priorité
                        "is_coherent": True,              # Flag cohérence
                        "metadata": {
                            "update_reason": "existing_zero_qty_with_real_qty",
                            "original_lot": numero_lot_original,
                            "quantite_theo_originale": 0,
                            "quantite_reelle_saisie": quantite_saisie,
                            "coherence_rule": "F_EQUALS_G_FOR_LOTECART",
                            "processing_priority": "LOTECART_EXISTING_UPDATE",
                            "validation_timestamp": pd.Timestamp.now().isoformat()
                        }
                    }
                    
                    updates.append(update)
                    
                    logger.info(
                        f"✅ MISE À JOUR LOTECART EXISTANTE: {code_article} "
                        f"(Lot original: '{numero_lot_original}' → 'LOTECART', "
                        f"Qté saisie: {quantite_saisie})"
                    )
                else:
                    logger.debug(
                        f"ℹ️ Ligne avec qté théo=0 mais qté réelle=0: {code_article} "
                        f"(pas de traitement LOTECART nécessaire)"
                    )
            
            logger.info(f"🎯 {len(updates)} mises à jour LOTECART pour lignes existantes")
            return updates
            
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour lignes LOTECART existantes: {e}", exc_info=True)
            return []
    
    def validate_lotecart_processing(
        self, 
        final_file_path: str, 
        expected_lotecart_count: int
    ) -> Dict[str, Any]:
        """
        Validation finale stricte du traitement LOTECART dans le fichier généré
        """
        validation_result = {
            "success": False,
            "lotecart_lines_found": 0,
            "correct_indicators": 0,
            "coherent_quantities": 0,
            "issues": [],
            "details": [],
            "critical_errors": []
        }
        
        try:
            if not final_file_path or not os.path.exists(final_file_path):
                validation_result["critical_errors"].append("Fichier final non trouvé")
                return validation_result
            
            logger.info(f"🔍 VALIDATION FINALE STRICTE LOTECART: {final_file_path}")
            
            # Analyser toutes les lignes LOTECART
            with open(final_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line.startswith('S;') and 'LOTECART' in line:
                        parts = line.split(';')
                        if len(parts) >= 15:
                            article = parts[8]
                            qty_f = parts[5]  # Colonne F (quantité théorique corrigée)
                            qty_g = parts[6]  # Colonne G (quantité réelle saisie)
                            indicateur = parts[7]
                            lot = parts[14]
                            
                            validation_result["lotecart_lines_found"] += 1
                            
                            # Validation stricte de l'indicateur
                            if indicateur == '2':
                                validation_result["correct_indicators"] += 1
                            else:
                                validation_result["critical_errors"].append(
                                    f"INDICATEUR INCORRECT ligne {line_num}: {article} "
                                    f"(indicateur={indicateur}, attendu=2)"
                                )
                            
                            # Validation stricte des quantités (F DOIT égaler G pour LOTECART)
                            try:
                                qty_f_val = float(qty_f)
                                qty_g_val = float(qty_g)
                                
                                if abs(qty_f_val - qty_g_val) < 0.001 and qty_f_val > 0 and qty_g_val > 0:
                                    validation_result["coherent_quantities"] += 1
                                else:
                                    validation_result["critical_errors"].append(
                                        f"QUANTITÉS INCOHÉRENTES ligne {line_num}: {article} "
                                        f"(F={qty_f}, G={qty_g}) - DOIT être F=G>0 pour LOTECART"
                                    )
                            except ValueError:
                                validation_result["critical_errors"].append(
                                    f"QUANTITÉS NON NUMÉRIQUES ligne {line_num}: {article} (F={qty_f}, G={qty_g})"
                                )
                            
                            # Validation du numéro de lot
                            if lot != "LOTECART":
                                validation_result["critical_errors"].append(
                                    f"NUMÉRO LOT INCORRECT ligne {line_num}: {article} "
                                    f"(lot={lot}, attendu=LOTECART)"
                                )
                            
                            # Ajouter aux détails
                            validation_result["details"].append({
                                "line": line_num,
                                "article": article,
                                "qty_f": qty_f,
                                "qty_g": qty_g,
                                "indicator": indicateur,
                                "lot": lot,
                                "status": "✅" if (
                                    indicateur == '2' and 
                                    abs(float(qty_f) - float(qty_g)) < 0.001 and 
                                    float(qty_f) > 0 and
                                    lot == "LOTECART"
                                ) else "❌"
                            })
            
            # Vérifications globales strictes
            if validation_result["lotecart_lines_found"] < expected_lotecart_count:
                validation_result["critical_errors"].append(
                    f"NOMBRE LOTECART INSUFFISANT: {validation_result['lotecart_lines_found']} < {expected_lotecart_count}"
                )
            
            if validation_result["correct_indicators"] < validation_result["lotecart_lines_found"]:
                validation_result["critical_errors"].append(
                    f"INDICATEURS INCORRECTS: {validation_result['correct_indicators']}/{validation_result['lotecart_lines_found']}"
                )
            
            if validation_result["coherent_quantities"] < validation_result["lotecart_lines_found"]:
                validation_result["critical_errors"].append(
                    f"QUANTITÉS INCOHÉRENTES: {validation_result['coherent_quantities']}/{validation_result['lotecart_lines_found']}"
                )
            
            # Succès STRICT: toutes les vérifications doivent passer
            validation_result["success"] = (
                len(validation_result["critical_errors"]) == 0 and
                validation_result["correct_indicators"] == validation_result["lotecart_lines_found"] and
                validation_result["coherent_quantities"] == validation_result["lotecart_lines_found"] and
                validation_result["lotecart_lines_found"] >= expected_lotecart_count
            )
            
            if validation_result["success"]:
                logger.info(
                    f"✅ VALIDATION FINALE LOTECART STRICTE RÉUSSIE: "
                    f"{validation_result['lotecart_lines_found']} lignes LOTECART parfaitement cohérentes"
                )
            else:
                logger.error(
                    f"❌ VALIDATION FINALE LOTECART STRICTE ÉCHOUÉE: "
                    f"{len(validation_result['critical_errors'])} erreur(s) critique(s)"
                )
                for error in validation_result["critical_errors"][:10]:  # Afficher max 10 erreurs
                    logger.error(f"   🔴 {error}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ Erreur validation finale LOTECART: {e}", exc_info=True)
            validation_result["critical_errors"].append(f"Erreur de validation: {str(e)}")
            return validation_result
    
    def get_lotecart_summary(
        self, 
        lotecart_candidates: pd.DataFrame,
        lotecart_adjustments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Génère un résumé détaillé et validé du traitement LOTECART
        """
        try:
            total_quantity = 0
            articles_by_inventory = {}
            priority_stats = {"new_lines": 0, "updated_lines": 0}
            validation_stats = {"coherent_adjustments": 0, "total_adjustments": len(lotecart_adjustments)}
            
            if not lotecart_candidates.empty:
                total_quantity = float(lotecart_candidates["Quantité Réelle"].sum())
                
                # Grouper par inventaire
                for _, row in lotecart_candidates.iterrows():
                    inv = row.get("Numéro Inventaire", "N/A")
                    if inv not in articles_by_inventory:
                        articles_by_inventory[inv] = []
                    
                    articles_by_inventory[inv].append({
                        "article": row["Code Article"],
                        "quantity": float(row["Quantité Réelle"]),
                        "lot_original": str(row.get("Numéro Lot", "")).strip()
                    })
            
            # Analyser les types d'ajustements et leur cohérence
            for adj in lotecart_adjustments:
                if adj.get("is_new_lotecart", False):
                    priority_stats["new_lines"] += 1
                elif adj.get("is_existing_line_update", False):
                    priority_stats["updated_lines"] += 1
                
                # Vérifier la cohérence de l'ajustement
                if adj.get("is_coherent", False):
                    validation_stats["coherent_adjustments"] += 1
            
            # Calcul du score de qualité
            quality_score = 0
            if validation_stats["total_adjustments"] > 0:
                quality_score = (validation_stats["coherent_adjustments"] / validation_stats["total_adjustments"]) * 100
            
            summary = {
                "candidates_detected": len(lotecart_candidates),
                "adjustments_created": len(lotecart_adjustments),
                "total_quantity": total_quantity,
                "inventories_affected": len(articles_by_inventory),
                "articles_by_inventory": articles_by_inventory,
                "priority_stats": priority_stats,
                "validation_stats": validation_stats,
                "quality_score": quality_score,
                "processing_timestamp": pd.Timestamp.now().isoformat(),
                "processing_mode": "STRICT_PRIORITY_LOTECART",
                "validation_status": "VALIDATED" if quality_score == 100 else "PARTIAL",
                "coherence_guaranteed": quality_score == 100
            }
            
            logger.info(
                f"📊 RÉSUMÉ LOTECART STRICT: "
                f"{summary['candidates_detected']} candidats, "
                f"{summary['adjustments_created']} ajustements, "
                f"{total_quantity} unités, "
                f"Score qualité: {quality_score:.1f}%"
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Erreur génération résumé LOTECART: {e}", exc_info=True)
            return {
                "candidates_detected": 0,
                "adjustments_created": 0,
                "total_quantity": 0,
                "inventories_affected": 0,
                "articles_by_inventory": {},
                "priority_stats": {"new_lines": 0, "updated_lines": 0},
                "validation_stats": {"coherent_adjustments": 0, "total_adjustments": 0},
                "quality_score": 0,
                "error": str(e),
                "processing_mode": "STRICT_PRIORITY_LOTECART",
                "validation_status": "ERROR"
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
            "validation_stats": {"coherent_adjustments": 0, "total_adjustments": 0},
            "quality_score": 100,  # 100% car pas de LOTECART à traiter
            "processing_timestamp": pd.Timestamp.now().isoformat(),
            "processing_mode": "STRICT_PRIORITY_LOTECART",
            "validation_status": "NO_LOTECART_DETECTED"
        }
    
    def reset_counter(self):
        """Remet à zéro le compteur LOTECART"""
        self.lotecart_counter = 0
        self.processed_lotecart = []
        logger.debug("🔄 Compteur et historique LOTECART remis à zéro")