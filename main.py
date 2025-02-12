from scripts import adv


def Update(bFullUpdate):
    adv.UpdateOriginalToDrive(bFullUpdate)

def Convert(bFullUpdate):
    adv.ConvertDriveToOutput(bFullUpdate)



def main():
    Convert(False)
    Update(False)