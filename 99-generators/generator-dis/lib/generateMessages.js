
export const generateMessages = (totalMsgs, intervalInMs, sendMsgFn) => {
  let msgCounter = 0
  
  const sendNextMsg = () => {
    if (msgCounter < totalMsgs) {
      sendMsgFn()
      msgCounter++
      setTimeout(sendNextMsg, intervalInMs)
      return true
    } else {
      return false
    }
  }

  // send the first message to activate the loop
  setTimeout(sendNextMsg, intervalInMs)
}