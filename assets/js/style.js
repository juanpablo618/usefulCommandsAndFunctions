'use strict';
/**
 * For old browsers
 */
var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * Preloader
 */
(function ($) {
  NProgress.start();

  $(window).on('load', function () {
    $('body').addClass('loaded');
    $('.preloader__circles').fadeOut('slow');

    NProgress.done();
  });
})(jQuery);

/**
 * Button main function in header section
 */
(function ($) {
  // Setup button keys

  var buttons = ['menu', 'date2', 'date', 'contacts'];
  // click handler
  var createClickHandler = function createClickHandler(value) {
    // set loop variables
    var selector = '#' + value,
        className = 'side_open_' + value;
    // create event triggers
    $(selector).on('click', function (e) {
      return $('html').toggleClass(className);
    });
    $('.header__menu a, .close').not(selector).on('click', function (e) {
      return $('html').removeClass(className);
    });
  };
  // iterate keys and apply handler method
  buttons.forEach(createClickHandler);
})(jQuery);

/**
 * Bootstrap dropdown.js
 * license MIT
 * @see http://getbootstrap.com/javascript/#dropdowns
 * @see https://github.com/twbs/bootstrap/blob/master/LICENSE
 */

(function ($) {
  var toggle = '[data-toggle="dropdown"]';

  var Dropdown = function () {
    function Dropdown(element) {
      _classCallCheck(this, Dropdown);

      $(element).on('click.bs.dropdown', this.toggle);
    }

    _createClass(Dropdown, [{
      key: 'toggle',
      value: function toggle(e) {
        var $this = $(this);

        if ($this.is('.disabled, :disabled')) {
          return;
        }

        var $parent = getParent($this);
        var isActive = $parent.hasClass('open');

        clearMenus();

        if (!isActive) {

          var relatedTarget = { relatedTarget: this };

          $parent.trigger(e = $.Event('show.bs.dropdown', relatedTarget));

          if (e.isDefaultPrevented()) {
            return;
          }

          $this.trigger('focus').attr('aria-expanded', 'true');

          $parent.toggleClass('open').trigger($.Event('shown.bs.dropdown', relatedTarget));
        }

        return false;
      }
    }, {
      key: 'keydown',
      value: function keydown(e) {
        if (!/(38|40|27|32)/.test(e.which) || /input|textarea/i.test(e.target.tagName)) {
          return;
        }

        var $this = $(this);

        e.preventDefault();
        e.stopPropagation();

        if ($this.is('.disabled, :disabled')) {
          return;
        }

        var $parent = getParent($this);
        var isActive = $parent.hasClass('open');

        if (!isActive && e.which != 27 || isActive && e.which === 27) {
          if (e.which === 27) {
            $parent.find(toggle).trigger('focus');
          }

          return $this.trigger('click');
        }

        var desc = ' li:not(.disabled):visible a';
        var $items = $parent.find('.dropdown-menu' + desc);

        if (!$items.length) {
          return;
        }

        var index = $items.index(e.target);

        if (e.which === 38 && index > 0) {
          index--;
        }

        if (e.which === 40 && index < $items.length - 1) {
          index++;
        }

        if (!~index) {
          index = 0;
        }

        $items.eq(index).trigger('focus');
      }
    }], [{
      key: '_jQueryInterface',
      value: function _jQueryInterface(options) {
        var _this = this;

        return this.each(function (i, target) {
          var $this = $(target);

          if (!$this.data('bs.dropdown')) {
            $this.data('bs.dropdown', new Dropdown(_this, options));
          }
        });
      }
    }]);

    return Dropdown;
  }();

  function getParent($this) {
    var selector = $this.attr('data-target');

    if (!selector) {
      selector = $this.attr('href');
      selector = selector && /#[A-Za-z]/.test(selector) && selector.replace(/.*(?=#[^\s]*$)/, '');
    }

    var $parent = selector && $(selector);

    return $parent && $parent.length ? $parent : $this.parent();
  }

  function clearMenus(e) {
    if (e && e.which === 3) {
      return;
    }

    $(toggle).each(function () {
      var $this = $(this);
      var $parent = getParent($this);
      var relatedTarget = { relatedTarget: this };

      if (!$parent.hasClass('open')) {
        return;
      };

      var isClick = e && e.type === 'click';

      if (isClick && /input|textarea/i.test(e.target.tagName) && $.contains($parent[0], e.target)) {
        return;
      }

      $parent.trigger(e = $.Event('hide.bs.dropdown', relatedTarget));

      if (e.isDefaultPrevented()) {
        return;
      }

      $this.attr('aria-expanded', 'false');
      $parent.removeClass('open').trigger($.Event('hidden.bs.dropdown', relatedTarget));
    });
  }

  $.fn.dropdown = Dropdown._jQueryInterface;

  $(document).on('click.bs.dropdown.data-api', clearMenus).on('click.bs.dropdown.data-api', toggle, Dropdown.prototype.toggle);
  // not close dropdown when click on him
  $('.dropdown__menu').on('click', function (event) {
    event.stopPropagation();
  });
})(jQuery);

function calcService(indata){
  var order = $(indata).data('id');
  var cantReceta = $('#r-cant'+order).val();
  var cantProducto = $('#p-cant'+order).val();
  var precioProducto = $('#p-valor'+order).val();
  if(cantReceta && cantProducto && precioProducto){
    var precioMl = precioProducto/cantProducto;
    var precioReceta = cantReceta*precioMl;
    precioReceta = precioReceta*1;
    $('#r-valor'+order).val(precioReceta.toFixed(2));
    //totales
    calcTotales();
  }
}

function calcTotales(){
  var unitarios = $('.tot-receta');
  var total = 0;
  for (var i = unitarios.length - 1; i >= 0; i--) {
    total = (total*1) + ($(unitarios[i]).val()*1);
  }      
  total = total*1;
  $('.tot-receta-t').html(total.toFixed(2));
}

(function ($) {
  $(document).on('change','.in-receta', function (event) {
    calcService(this);
    event.stopPropagation();    
  });
})(jQuery);

(function ($) {
   $(document).on('click','.close-item', function (event) {
    event.stopPropagation();
    var id = $(this).data('id');
    $('.dp'+id).remove();
    calcTotales();
  });
})(jQuery);
(function ($) {
  $('.add-prod').on('click', function (event) {
    var num = $('#counter-rec').val();
    var block = '<h4 class="title-mini dp'+num+'">Producto</h4><h4 class="menu__title tit-h4 dp'+num+'"><input id="name'+num+'" type="text" name="product-name" class="prod-name"><span data-id="'+num+'" class="close-item dp'+num+'">×</span></h4>'
        +'<div class="menu__item dropdown dp'+num+'">'
          +'<div data-toggle="dropdown">'
            +'<i class="gc gc-water"></i>'
            +'<div class="menu__info">'
              +'<h4 class="title-mini">Presentación</h4>'
              +'<span><input type="number" name="p-cant0" data-id="'+num+'" id="p-cant'+num+'" min="0" class="in-receta">ml</span>'
              +'<span>$<input type="number" name="p-valor0" data-id="'+num+'" id="p-valor'+num+'" min="0" class="in-receta"></span>'
            +'</div>'
          +'</div>'
        +'</div>'
        +'<div class="menu__item dropdown dp'+num+'">'
          +'<div data-toggle="dropdown">'
            +'<i class="gc gc-glass"></i>'
            +'<div class="menu__info">'
              +'<h4 class="title-mini">Receta</h4>'
              +'<span><input type="number" data-id="'+num+'" name="r-cant'+num+'" id="r-cant'+num+'" min="0" class="in-receta">ml</span>'
              +'<span>$<input readonly="readonly" data-id="'+num+'" type="number" name="r-valor'+num+'" id="r-valor'+num+'" class="in-receta tot-receta"></span>'
            +'</div>'
          +'</div>'
        +'</div>';
        num = num*1;
        num = num+1;
        $('#counter-rec').val(num);
        $( ".add-prod" ).before( block );
  });
  })(jQuery);