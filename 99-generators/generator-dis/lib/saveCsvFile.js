
export const saveCsvFile = async (file, metricsArr) => {
  try {
    await file.write(metricsArr.join('\n') + '\n')  
    console.log('current metrics saved...')
    return true    
  } catch (error) {
    console.log('Erros saving file:', error)
    return false
  }  
}