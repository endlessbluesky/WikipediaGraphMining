#include <stdio.h>
#include <stdlib.h>
#include <cuda_runtime.h>
//#include <cusparse.h>
#include <cusparse_v2.h>
#include <iostream>

int main() {
	cudaError_t cudaStat1,cudaStat2,cudaStat3,cudaStat4,cudaStat5,cudaStat6;
    cusparseStatus_t status;
    cusparseHandle_t handle=0;
    cusparseMatDescr_t descra=0;
    int *    cooRowIndexHostPtr=0;
    int *    cooColIndexHostPtr=0;    
    double * cooValHostPtr=0;
    int *    cooRowIndex=0;
    int *    cooColIndex=0;    
    double * cooVal=0;
    int *    xIndHostPtr=0;
    double * xValHostPtr=0;
    double * yHostPtr=0;
    int *    xInd=0;
    double * xVal=0;
    double * y=0;  
    int *    csrRowPtr=0;
    double * zHostPtr=0; 
    double * z=0; 
    int      n, nnz, nnz_vector, i, j;

    printf("testing example\n");
    /* create the following sparse test matrix in COO format */
     /* |1.0     2.0 3.0|
       	|    4.0        |
       	|5.0     6.0 7.0|
       	|    8.0     9.0| */

    n=4; nnz=9; 
    cooRowIndexHostPtr = (int *)   malloc(nnz*sizeof(cooRowIndexHostPtr[0])); 
    cooColIndexHostPtr = (int *)   malloc(nnz*sizeof(cooColIndexHostPtr[0])); 
    cooValHostPtr      = (double *)malloc(nnz*sizeof(cooValHostPtr[0])); 

    cooRowIndexHostPtr[0]=0; cooColIndexHostPtr[0]=0; cooValHostPtr[0]=1.0;  
    cooRowIndexHostPtr[1]=0; cooColIndexHostPtr[1]=2; cooValHostPtr[1]=2.0;  
    cooRowIndexHostPtr[2]=0; cooColIndexHostPtr[2]=3; cooValHostPtr[2]=3.0;  
    cooRowIndexHostPtr[3]=1; cooColIndexHostPtr[3]=1; cooValHostPtr[3]=4.0;  
    cooRowIndexHostPtr[4]=2; cooColIndexHostPtr[4]=0; cooValHostPtr[4]=5.0;  
    cooRowIndexHostPtr[5]=2; cooColIndexHostPtr[5]=2; cooValHostPtr[5]=6.0;
    cooRowIndexHostPtr[6]=2; cooColIndexHostPtr[6]=3; cooValHostPtr[6]=7.0;  
    cooRowIndexHostPtr[7]=3; cooColIndexHostPtr[7]=1; cooValHostPtr[7]=8.0;  
    cooRowIndexHostPtr[8]=3; cooColIndexHostPtr[8]=3; cooValHostPtr[8]=9.0;  

    //print the matrix
    printf("Input data:\n");
    for (i=0; i<nnz; i++){        
        printf("cooRowIndexHostPtr[%d]=%d  ",i,cooRowIndexHostPtr[i]);
        printf("cooColIndexHostPtr[%d]=%d  ",i,cooColIndexHostPtr[i]);
        printf("cooValHostPtr[%d]=%f     \n",i,cooValHostPtr[i]);
    }

    status = cusparseCreate(&handle);
    status = cusparseCreateMatDescr(&descra);
    cusparseSetMatType(descra, CUSPARSE_MATRIX_TYPE_GENERAL);
    cusparseSetMatIndexBase(descra, CUSPARSE_INDEX_BASE_ONE);

	cudaError_t cudaErrorCode;
    cudaErrorCode = cudaMalloc((void**)&csrRowPtr,(n+1)*sizeof(csrRowPtr[0]));
    status = cusparseXcoo2csr(handle, cooRowIndex,nnz, n, 
    	csrRowPtr, CUSPARSE_INDEX_BASE_ZERO);

    std::cout << "cuda error code = " << cudaErrorCode << std::endl;
    std::cout << "cusparseCreate return status = " << status << std::endl;
    return 0;
}