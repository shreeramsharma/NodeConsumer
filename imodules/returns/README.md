## Installation Steps - RETURNS
### WINDOWS

-   Install Python 3.7.6. 
-   While installing select the checkbox(`Add Python 3.7 to PATH`). Reference URL : `https://docs.python.org/3/using/windows.html`
-   Download the folder `wheelhouse_windows` using the following link   
        
        https://drive.google.com/drive/folders/1z41th3Z9XIa4D--1coPQsi1dVdB5uy0g 
-   Copy all the files from `wheelhouse_windows` to `microservice/nconsumer/imodules/returns` folder.
-   Open command prompt and navigate to `microservice/nconsumer/imodules/returns` folder.
-   Run the following command  
    
        `pip install -r requirements.txt --no-index --find-links wheelhouse_windows`

### Linux
-   Download the file `wheelhouse_linux.zip` using the following link  

        https://drive.google.com/drive/folders/1z41th3Z9XIa4D--1coPQsi1dVdB5uy0g
        
-   Unzip wheelhouse_linux.zip and copy the files to `microservice/nconsumer/imodules/returns/` dir
-   Open terminal and traverse to `microservice/nconsumer/imodules/returns/` dir
-   Run the following to install all packages  
    
        pip install -r requirements.txt --no-index --find-links wheelhouse_linux

