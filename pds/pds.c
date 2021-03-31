#include<stdio.h>
#include<string.h>
#include<errno.h>
#include<stdlib.h>
#include "bst.h"
#include "pds.h"
#include "contact.h"
// define the global variable
struct PDS_RepoInfo repo_handle;

int pds_open( char *repo_name, int rec_size )
{
    int status;
    char repo_file[30];
    char ndx_file[30];
    if(repo_handle.repo_status==PDS_REPO_OPEN)
    {
        return PDS_REPO_ALREADY_OPEN;
    }
    // If repo_name is "demo", then data file should be "demo.dat"
    strcpy(repo_handle.pds_name,repo_name);
    strcpy(repo_file,repo_name);
    strcat(repo_file,".dat");
    strcpy(ndx_file,repo_name);
    strcat(ndx_file,".ndx");

    // Open the data file in rb+ mode
    // rb+ means open an existing file for update (reading and writing) in binary mode
    repo_handle.pds_data_fp=(FILE *) fopen(repo_file,"rb+");
    if(repo_handle.pds_data_fp==NULL)
    {
        perror(repo_file);
    }
    // Open the index file in rb+ mode
    repo_handle.pds_ndx_fp=(FILE *) fopen(ndx_file,"rb+");
    if(repo_handle.pds_ndx_fp==NULL)
    {
        perror(ndx_file);
    }
    // Initialize other members of PDS_RepoInfo
    repo_handle.repo_status=PDS_REPO_OPEN;
    repo_handle.rec_size=rec_size;
    repo_handle.pds_bst=NULL;
    int count=0;
    fseek(repo_handle.pds_data_fp,0,SEEK_END);
    // if index file is empty
    if(ftell(repo_handle.pds_data_fp)==0)
    {
        //initialise free_list to  -1
        for (int i=0;i<MAX_FREE;i++)
        {
            repo_handle.free_list[i]=-1;
        }
    }
    // else index files contains free list
    else
    {
        // initialise free_list from index file
        for(int i=0;i<MAX_FREE;i++)
        {
            fread(&repo_handle.free_list[i], sizeof(int), 1, repo_handle.pds_ndx_fp);
        }
    }
    // Internal function used by pds_open to read index entries into BST
    status=pds_load_ndx();
    // Close the index file
    fclose(repo_handle.pds_ndx_fp);
    return PDS_SUCCESS;
}

// Internal function used by pds_open to read index entries into BST
int pds_load_ndx()
{
    int status;
    struct PDS_NdxInfo *index_entry;
    while(!feof(repo_handle.pds_ndx_fp))
    {   
        index_entry=(struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
        // Build BST and store in pds_bst by reading index entries from the index file
        fread(index_entry,sizeof(struct PDS_NdxInfo),1,repo_handle.pds_ndx_fp);
        status=bst_add_node(&repo_handle.pds_bst,index_entry->key,index_entry);
    }
    return status;
}

int put_rec_by_key( int key, void *rec )
{
    int offset,status,writesize1,writesize2,available_del=0,free_location;
    struct PDS_NdxInfo *index_entry;
    // Seek to the end of the data file
    fseek(repo_handle.pds_data_fp, 0, SEEK_END);
    // Check if there are any deleted location available in the free list
    for(int i=0;i<MAX_FREE;i++)
    {
        if(repo_handle.free_list[i]!=-1)
        {
            fseek(repo_handle.pds_data_fp,repo_handle.free_list[i], SEEK_SET);
            repo_handle.free_list[i] = -1;
            break;
        }
    }
    offset=ftell(repo_handle.pds_data_fp);
    // Create an index entry with the current file location
    index_entry=(struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
    index_entry->key=key;
    index_entry->offset=offset;
    // Add index entry to BST
    status=bst_add_node(&repo_handle.pds_bst,key,index_entry);
    if(status!=BST_SUCCESS)
    {
        fprintf(stderr,"unable to add index entry for key %d - Error %d\n",key,status);
        free(index_entry);
        fseek(repo_handle.pds_data_fp,offset,SEEK_SET);
        status=PDS_ADD_FAILED;
    }
    else
    {
        // 1. Write the key at the current file location
        writesize1=fwrite(&key,sizeof(int),1,repo_handle.pds_data_fp);
        // 2. Write the record at the current file location
        writesize2=fwrite(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
        status=PDS_SUCCESS;
    }

    return status;
}

int get_rec_by_ndx_key( int key, void *rec )
{
    int status,offset,readsize;
    struct PDS_NdxInfo *index_entry;
    struct BST_Node *bst_node;
    // Search for index entry in BST
    bst_node=bst_search(repo_handle.pds_bst,key);
    if(bst_node==NULL)
    {
        status=PDS_REC_NOT_FOUND;
    }
    else
    {
        index_entry=(struct PDS_NdxInfo*) bst_node->data;
        offset=index_entry->offset;
        // Seek to the file location based on offset in index entry
        fseek(repo_handle.pds_data_fp,offset,SEEK_SET);
        int value_key;
        // 1. Read the key at the current file location 
        fread(&value_key,sizeof(int),1,repo_handle.pds_data_fp);
        // 2. Read the record after reading the key
        readsize=fread(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
        status=PDS_SUCCESS;
    }

    return status;
}

// Search based on a key field on which an index does not exist. 
// This function actually does a full table scan by reading the data file until the desired record is found.
int get_rec_by_non_ndx_key( void *key,void *rec,int (*matcher)(void *rec, void *key),int *io_count)
{
    int status,flag=0;
    // The io_count is an output parameter to indicate the number of records.
    *io_count=0;
    fseek(repo_handle.pds_data_fp,0,SEEK_SET);
    int *readkey=(int *)malloc(sizeof(int));

    while(!feof(repo_handle.pds_data_fp))
    {   
        int check_offset=0;
        // Check if data at the current offset is deleted from data file
        for(int i=0;i<MAX_FREE;i++)
        {
            if(repo_handle.free_list[i]==ftell(repo_handle.pds_data_fp))
            {
                check_offset=1;
                break;
            }
        }
        // if the data at the current offset is present in data file
        if(check_offset==0)
        {
            fread(readkey,sizeof(int),1,repo_handle.pds_data_fp);
            fread(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
            // incrementing io_count by 1 after each read only if the current offset is not deleted
            *io_count+=1;
            if(matcher(rec,key)==0)
            {
                fseek(repo_handle.pds_data_fp,0,SEEK_END);
                // if record is found then status= PDS_SUCCEESS
                status=PDS_SUCCESS;
                return status;
            }
        }
        // else data at the current offset is deleted from data file
        else
        {
            fseek(repo_handle.pds_data_fp,(sizeof(int)+repo_handle.rec_size),SEEK_CUR);
        }
    }
    status=PDS_REC_NOT_FOUND;

    return status;
}

// update
int update_by_key( int key, void *newrec )
{
    int status,offset;
    struct PDS_NdxInfo *index_entry;
    struct BST_Node *bst_node;
    // Search for index entry in BST
    bst_node=bst_search(repo_handle.pds_bst,key);
    if(bst_node==NULL)
    {
        status=PDS_MODIFY_FAILED;
    }
    else
    {
        index_entry=(struct PDS_NdxInfo*) bst_node->data;
        offset=index_entry->offset;
        // Seek to the file location based on offset in index entry
        fseek(repo_handle.pds_data_fp,offset,SEEK_SET);
        fwrite(&key,sizeof(int),1,repo_handle.pds_data_fp);
        // Overwrite the existing record with the given record
        fwrite(newrec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
        status=PDS_SUCCESS;
    }

    return status;
}

// pds_delete
int delete_by_key( int key )
{
    int status,offset,readsize;
    struct PDS_NdxInfo *index_entry;
    struct BST_Node *bst_node;
    // Search for index entry in BST
    bst_node=bst_search(repo_handle.pds_bst,key);
    if(bst_node==NULL)
    {
        status=PDS_DELETE_FAILED;
    }
    else
    {
        index_entry=(struct PDS_NdxInfo*) bst_node->data;
        offset=index_entry->offset;
        int flag=0;
        // store the offset value in to free list
        for(int i=0;i<MAX_FREE;i++)
        {
            if(repo_handle.free_list[i]==-1)
            {
                flag=1;
                repo_handle.free_list[i]=offset;
                // Delete the key from BST
                bst_del_node(&repo_handle.pds_bst,key);
                status=PDS_SUCCESS;
                break;
            }
        }
        // if PDS delete failed if freelist has no empty slot
        if(flag==0)
        {
            status=PDS_DELETE_FAILED;
        }
    }
    return status;
}

// Internal function used by pds_close to unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
void preorder(struct BST_Node *node)
{
    if(node==NULL)
    {
        return;
    }
    fwrite(node->data,sizeof(struct PDS_NdxInfo),1,repo_handle.pds_ndx_fp);
    preorder(node->left_child);
    preorder(node->right_child);
}

// Close all files and free the BST
int pds_close()
{
    int status;
    char ndx_file[30];
    // If repo_name is "demo", then data file should be "demo.dat"
    strcpy(ndx_file,repo_handle.pds_name);
    strcat(ndx_file,".ndx");
    // Open the index file in wb mode (write mode, not append mode)
    repo_handle.pds_ndx_fp=(FILE *) fopen(ndx_file,"wb");
    // write the free list into the index file before closing
    for(int i=0;i<MAX_FREE;i++)
    {
        fwrite(&repo_handle.free_list[i],sizeof(int),1,repo_handle.pds_ndx_fp);
    }
    // Internal function used by pds_close to unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
    preorder(repo_handle.pds_bst);
    // Free the BST by call bst_destroy()
    bst_destroy(repo_handle.pds_bst);
    strcpy(repo_handle.pds_name,"");
    // Close the index file
    fclose(repo_handle.pds_ndx_fp);
    // Close the data file
    fclose(repo_handle.pds_data_fp);
    repo_handle.repo_status=PDS_REPO_CLOSED;
    status=PDS_SUCCESS;

    return status;
}