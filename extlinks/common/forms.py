from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from django import forms
from django.core.exceptions import ValidationError


class MonthInput(forms.DateInput):
    input_type = "month"


class YearMonthField(forms.DateField):
    def to_python(self, value):
        """
        This is automatically called by Django before validation (clean() method).
        """
        if not value:
            return None

        try:
            year, month = map(int, value.split("-")[:2])
            return date(year, month, 1)
        except ValueError:
            raise ValidationError("Enter a valid year-month")


class FilterForm(forms.Form):

    start_date = YearMonthField(
        required=False,
        label="Start date:",
        widget=MonthInput(attrs={"class": "form-control"}),
    )
    end_date = YearMonthField(
        required=False,
        label="End date:",
        widget=MonthInput(attrs={"class": "form-control"}),
    )

    limit_to_user_list = forms.BooleanField(required=False)

    namespace_id = forms.IntegerField(
        required=False,
        label="Namespace ID:",
        widget=forms.NumberInput(
            attrs={"class": "form-control", "style": "width: 6rem;"}
        ),
    )

    exclude_bots = forms.BooleanField(required=False)

    def clean_end_date(self):
        """
        This is automatically called by Django when validating this field.

        Modify the end date to return the last day of its month.
        """
        end_date = self.cleaned_data.get("end_date")
        
        if not end_date:
            return None
        
        next_month = end_date.replace(day=1) + relativedelta(months=1)

        return next_month - timedelta(days=1)
