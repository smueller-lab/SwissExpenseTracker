import subprocess
import sys

def run_script(pth: str, q_Redo: bool = False):
    """Runs a python script from Pipeline with q_Redo default to false

    Args:
        pth (str): path of the python script
        q_Redo (bool, optional): If script should be rerun with all data. Defaults to False.
    """
    print(f"▶ Running: {pth}")
    result = subprocess.run(
        [sys.executable, pth, f"--q_Redo={str(q_Redo)}"],
    )
    if result.returncode != 0:
        print(f"❌ Error in {pth}")
        sys.exit(result.returncode)
    print(f"✅ Done: {pth}\n")


def main():
    print("=== Starting full pipeline ===\n")

    print("=== Start DebitCard import ===\n")
    run_script("Pipeline/DebitCard/01_ing_DebitCard.py")
    run_script("Pipeline/DebitCard/02_imp_DebitCard.py")
    run_script("Pipeline/DebitCard/03_rfn_DebitCard.py")
    run_script("Pipeline/DebitCard/04_mrg_DebitCard.py")
    run_script("Pipeline/DebitCard/05_use_unify_subjectCategories.py")

    print("=== Start Viseca import ===\n")

    run_script("Pipeline/Viseca/01_ing_Viseca.py")
    run_script("Pipeline/Viseca/02_imp_Viseca.py")
    run_script("Pipeline/Viseca/03_rfn_Viseca.py")
    run_script("Pipeline/Viseca/04_mrg_Viseca.py")

    run_script("Pipeline/mrg/01_mrg_DebitCard_Viseca.py")

    print("🎉 All steps completed!")


if __name__ == "__main__":
    main()