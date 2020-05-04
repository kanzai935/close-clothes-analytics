const puppeteer = require('puppeteer');

async function scrollToBottom(page, viewportHeight) {
  const getScrollHeight = () => {
    return Promise.resolve(document.documentElement.scrollHeight) }

  let scrollHeight = await page.evaluate(getScrollHeight)
  let currentPosition = 0
  let scrollNumber = 0

  while (currentPosition < scrollHeight) {
    scrollNumber += 1
    const nextPosition = scrollNumber * viewportHeight
    await page.evaluate(function (scrollTo) {
      return Promise.resolve(window.scrollTo(0, scrollTo))
    }, nextPosition)
    await page.waitForNavigation({waitUntil: 'networkidle2', timeout: 5000})
              .catch(e => console.log('timeout exceed. proceed to next operation'));

    currentPosition = nextPosition;
    scrollHeight = await page.evaluate(getScrollHeight)
    }
}

(async () => {

  var sex = process.argv[2]
  var category = process.argv[3]

  const viewportHeight = 600
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto('http://zozo.jp/' + sex +'-category/' + category + '/');
  await page.waitForNavigation({waitUntil: 'networkidle2', timeout: 5000})
            .catch(e => console.log('timeout exceed. proceed to next operation'));
  await scrollToBottom(page, viewportHeight)
  itemsSelector = '#searchResultList > li > div.listInner';
  imagesSelector = itemsSelector + ' > div.thumb > a > img';
  brandsSelector = itemsSelector + ' > div > p.brand';

  var brands = await page.$$eval(brandsSelector, list => {
      return list.map(data => data.textContent);
  });
  var images = await page.$$eval(imagesSelector, list => {
      return list.map(data => data.src);
  });
  var AWS = require('aws-sdk');
  AWS.config.region = 'us-east-1'
  var sqs = new AWS.SQS();


  var queueUrl = "https://sqs.us-east-1.amazonaws.com/234514069284/zozo-image-upload";
  for(var i = 0; i < images.length; i++) {
      var obj = {
        "image_url": images[i],
        "brand": brands[i],
        "sex": sex,
        "category": category
      };
      var params = {
              MessageBody: JSON.stringify(obj),
              QueueUrl: queueUrl
      };
      sqs.sendMessage(params, function(err, data) {
              if (err) {
                  console.log(err, err.stack);
              }
      });
  }
  await browser.close()
})();

